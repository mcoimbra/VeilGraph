package pt.ulisboa.tecnico.graph.model.randomwalk;


import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import pt.ulisboa.tecnico.graph.model.AbstractGraphModel;
import pt.ulisboa.tecnico.graph.stream.GraphStreamHandler;
import pt.ulisboa.tecnico.graph.stream.GraphUpdateTracker;
import pt.ulisboa.tecnico.graph.util.GraphUtils;



/**
 * Class to build the summary graph, given the original graph and a set of "hot" vertices
 *
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @author Miguel E. Coimbra
 */
@SuppressWarnings("serial")
public class BigVertexGraph<VV, EV> extends AbstractGraphModel<Long, VV, EV, Tuple2<Long, Double>> {
	private static final long serialVersionUID = -8025093176176897258L;

    // Graph representation as a big vertex graph.
    private Graph<Long, Double, Double> currentGraphRepresentation = null;
    private VertexKeySelector<Long> keySelector;
    private TypeInformation<Edge<Long, Double>> edgeTypeInfo;
    private TypeInformation<Tuple2<Long, Double>> tuple2TypeInfo;

    // Big vertex graph parameters.
    private Double updateRatioThreshold;
    private Integer neighborhoodSize;
    private Double delta;
    private Double initialRank = 1.0d; // TODO: perhaps this should move to PageRankStreamHandler.

    // Big vertex graph statistics.
    private Long deltaExpansionLimit = -1L;
    private long paramSetup = -1L;
    private String internalEdgeCount = null;
    private String externalEdgeCount = null;
    private String ranksToSendCount = null;
    private String edgesToInsideCount = null;

    public void updateGraph(Boolean dumpingModel, Long iteration) {
        this.dumpingModel = dumpingModel;
        this.iteration = iteration;
    }





    public Long getSetupTime() {
        return this.paramSetup;
    }

    public Graph<Long, Double, Double> getGraph(final ExecutionEnvironment env, Graph<Long, NullValue, NullValue> graph, final DataSet<Tuple2<Long, GraphUpdateTracker.UpdateInfo>> infoDataSet, DataSet<Tuple2<Long, Double>> previousResults) throws Exception {

        final DataSet<Long> expandedVertices = this.expand(env, graph, infoDataSet, previousResults, EdgeDirection.IN);

        final Graph<Long, Double, Double> summaryGraph = this.summaryGraph(expandedVertices, previousResults, graph);

        // Prepare big vertex graph's edge spill to disk.
        this.edgeTypeInfo = summaryGraph.getEdges().getType();
        final TypeSerializerOutputFormat<Edge<Long, Double>> edgeOutputFormat = new TypeSerializerOutputFormat<>();
        edgeOutputFormat.setInputType(edgeTypeInfo, env.getConfig());
        edgeOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        edgeOutputFormat.setOutputFilePath(new Path(this.modelDirectory + "/edges"));
        summaryGraph
                .getEdges()
                .output(edgeOutputFormat)
                .name("BigVertexGraph - write edges to disk.");

        // Prepare big vertex graph's vertex spill to disk.
        final TypeInformation<Vertex<Long, Double>> vertexTypeInfo = summaryGraph.getVertices().getType();
        final TypeSerializerOutputFormat<Vertex<Long, Double>> vertexOutputFormat = new TypeSerializerOutputFormat<>();
        vertexOutputFormat.setInputType(vertexTypeInfo, env.getConfig());
        vertexOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        vertexOutputFormat.setOutputFilePath(new Path(this.modelDirectory + "/vertices"));
        summaryGraph
                .getVertices()
                .output(vertexOutputFormat)
                .name("BigVertexGraph - write vertices to disk.");

        // Trigger the second and last phase of the big vertex graph creation as a Flink job.
        env.execute("BigVertexGraph - write to disk.");

        final JobExecutionResult res = env.getLastJobExecutionResult();

        final Long internal_edge_count = res.getAccumulatorResult(this.internalEdgeCount);
        super.statisticsMap.get(RandomWalkStatisticKeys.INTERNAL_EDGE_COUNT.toString()).add(internal_edge_count);

        final Long external_edge_count = res.getAccumulatorResult(this.externalEdgeCount);
        super.statisticsMap.get(RandomWalkStatisticKeys.EXTERNAL_EDGE_COUNT.toString()).add(external_edge_count);

        Long values_to_send = res.getAccumulatorResult(this.ranksToSendCount);
        super.statisticsMap.get(RandomWalkStatisticKeys.VALUES_TO_SEND.toString()).add(values_to_send);

        Long edges_to_inside_count = res.getAccumulatorResult(this.edgesToInsideCount);
        super.statisticsMap.get(RandomWalkStatisticKeys.EDGES_TO_INSIDE.toString()).add(edges_to_inside_count);

        // Add to paramSetup the time it took to build the big vertex graph and to spill it to disk.
        this.paramSetup = this.paramSetup + res.getNetRuntime(TimeUnit.MILLISECONDS);


        // Must copy the added edges into the DataSet degreeDataset
        final TypeSerializerInputFormat<Edge<Long, Double>> edgeInputFormat = new TypeSerializerInputFormat<>(edgeTypeInfo);
        edgeInputFormat.setFilePath(this.modelDirectory + "/edges");
        final DataSet<Edge<Long, Double>> sinkEdges = env.createInput(edgeInputFormat, edgeTypeInfo);

        final TypeSerializerInputFormat<Vertex<Long, Double>> vertexInputFormat = new TypeSerializerInputFormat<>(vertexTypeInfo);
        vertexInputFormat.setFilePath(this.modelDirectory + "/vertices");
        final DataSet<Vertex<Long, Double>> sinkVertices = env.createInput(vertexInputFormat, vertexTypeInfo);


        final Graph<Long, Double, Double> summaryGraphFromDisk = Graph.fromTupleDataSet(
                sinkVertices.map(new MapFunction<Vertex<Long, Double>, Tuple2<Long, Double>>() {

                    @Override
                    public Tuple2<Long, Double> map(Vertex<Long, Double> value) {
                        return value;
                    }
                }),
                sinkEdges.map(new MapFunction<Edge<Long, Double>, Tuple3<Long, Long, Double>>() {
                    @Override
                    public Tuple3<Long, Long, Double> map(Edge<Long, Double> value) {
                        return value;
                    }
                }),
                env);

        return summaryGraphFromDisk;
    }


    //@FunctionAnnotation.NonForwardedFields("f1.f0; f1.f2")
    @FunctionAnnotation.ForwardedFields("f1.f1->f0")
    //@FunctionAnnotation.ReadFields("f0.f1; f1")
    public static class NeighborhoodHopper implements FlatJoinFunction<Tuple2<Long, Long>, Edge<Long, NullValue>, Tuple2<Long, Long>> {

        @Override
        public void join(
                Tuple2<Long, Long> f0,
                Edge<Long, NullValue> f1,
                Collector<Tuple2<Long, Long>> out) {
            if(f1 != null) {
                // Store the out-vertex with a score equal to the current vertex minus 1.
                Tuple2<Long, Long> wsNewElement = Tuple2.of(f1.f1, f0.f1 - 1);
                out.collect(wsNewElement);
            } /*
			else { // 2018-03-29 ADDED FOR TESTING
				// This is for the case of expanding on a sink vertex (has no out-neighbors)
				Tuple2<Long, Long> wsNewElement = Tuple2.of(first.f0, 0L);
				out.collect(wsNewElement);
			} */
        }
    }


    public void setTypeInfo(TypeInformation<Long> keyTypeInfo, TypeInformation<Edge<Long, Double>> edgeTypeInfo, TypeInformation<Tuple2<Long, Double>> tupleTypeInfo) {
        this.keySelector = new VertexKeySelector<>(keyTypeInfo);
        this.edgeTypeInfo = edgeTypeInfo;
        this.tuple2TypeInfo = tupleTypeInfo;
    }

    public BigVertexGraph(final Double updateRatioThreshold, final Integer neighborhoodSize, final Double delta) {
        this.updateRatioThreshold = updateRatioThreshold;
        this.neighborhoodSize = neighborhoodSize;
        this.delta = delta;
    }

    /**
     * Get all vertices whose degree changed at least r as a consequence of the last applied update.
     * The user specifies if the target is the in-degree (EdgeDirection.IN), out-degree (EdgeDirection.OUT) or either of them (EdgeDirection.ALL).
     *
     * @see org.apache.flink.graph.EdgeDirection
     *
     * @param infos accumulated updates
     * @param r minimum vertex degree change
     * @param direction in-degree, out-degree or both
     * @return a list of vertices whose specified type of degree had a delta of at least r since the last update
     */
    /*
    public static List<Tuple2<Long, Double>> getKHotVertices(final Map<Long, GraphUpdateTracker.UpdateInfo> infos, final Double r, final EdgeDirection direction) {

        return infos
                .entrySet()
                .stream() // this filter + map chain could be replaced by a single operator which filters and collects if update ratio > r
                .filter((Map.Entry<Long, GraphUpdateTracker.UpdateInfo> e) -> {
                    final GraphUpdateTracker.UpdateInfo i = e.getValue();
                    return checkVertexDirectionDegreeRatio(r, direction, i);
                })
                .map(new Function<Map.Entry<Long, GraphUpdateTracker.UpdateInfo>, Tuple2<Long, Double>>() {
                    public Tuple2<Long, Double> apply(Map.Entry<Long, GraphUpdateTracker.UpdateInfo> e) {
                        final GraphUpdateTracker.UpdateInfo u = e.getValue();
                        switch (direction) {
                            case IN:
                                return Tuple2.of(e.getKey(), GraphUpdateTracker.degreeUpdateRatio(u.prevInDegree, u.currInDegree));
                            case OUT:
                                return Tuple2.of(e.getKey(), GraphUpdateTracker.degreeUpdateRatio(u.prevOutDegree, u.currOutDegree));
                            default:
                                return GraphUpdateTracker.degreeUpdateRatio(u.prevInDegree, u.currInDegree) > r ?
                                        Tuple2.of(e.getKey(), GraphUpdateTracker.degreeUpdateRatio(u.prevInDegree, u.currInDegree)) :
                                        Tuple2.of(e.getKey(), GraphUpdateTracker.degreeUpdateRatio(u.prevOutDegree, u.currOutDegree));
                        }

                }})
                .collect(Collectors.toList());
    }
    */

    private static boolean checkVertexDirectionDegreeRatio(Double r, EdgeDirection direction, GraphUpdateTracker.UpdateInfo i) {
        switch (direction) {
            case IN:
                return GraphUpdateTracker.degreeUpdateRatio(i.prevInDegree, i.currInDegree) > r;
            case OUT:
                return GraphUpdateTracker.degreeUpdateRatio(i.prevOutDegree, i.currOutDegree) > r;
            default:
                return GraphUpdateTracker.degreeUpdateRatio(i.prevInDegree, i.currInDegree) > r ||
                            GraphUpdateTracker.degreeUpdateRatio(i.prevOutDegree, i.currOutDegree) > r;
        }
    }

    /**
     * TODO DESCRIPTION.
     * @param expandedIds
     * @param graph
     * @param maxDeltaIterations
     * @return
     */
    private DataSet<Long> deltaExpansion(DataSet<Tuple2<Long, Long>> expandedIds, Graph<Long, NullValue, NullValue> graph, Integer maxDeltaIterations) {

        // If there is no vertex upon which to expand, return the current vertex ids.
        if(maxDeltaIterations == 0) {
            return expandedIds.map(new VertexIdExtractor());
        }

        final int keyPosition = 0;

        //TODO: test iterateDelta but set the initialDeltaSet to be equal to an initial expansion. THis means that we would do the same operations in the DeltaIteration body to initialize the initialDeltaSet, which would be fed to the DeltaIteration.

        // https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/index.html#iteration-operators
        final DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> deltaIteration =
                expandedIds.iterateDelta(expandedIds, maxDeltaIterations, keyPosition);

        // Retain only vertices that are to be expanded: their value is greater than zero (0).
        final DataSet<Tuple2<Long, Long>> positiveHops = deltaIteration
                .getWorkset()
                .filter(new KeepRanksAbove(0L))
                .name("DeltaIteration Workset Keep Above 0");

        // Join current vertices with out-edges.
        // This represents the out-neighbors of the vertices in positiveHops.
        final DataSet<Tuple2<Long, Long>> outVertices = positiveHops
                //.leftOuterJoin(Graph.fromTuple2DataSet(env.createInput(this.edgeInputType), env).getEdges())
                .leftOuterJoin(graph.getEdges()) // get out-edges
                .where(0)
                .equalTo(0)
                .with(new NeighborhoodHopper()) // move score to out-neighbors
                .groupBy(0)
                .reduce(new HopNormalizer()) // if an out-neighbor is reached by more than one vertex, keep the highest hops remaining
                .name("DeltaIteration positiveHops HopNormalizer");



        new RepeatedExpansionNormalizer();

        // If a vertex in the current working set was reached in the expansion and its new expansion limit is bigger, store it.
        // repeatedExpansionVertices could be empty.
        final DataSet<Tuple2<Long, Long>> repeatedExpansionVertices = positiveHops
                .join(outVertices)
                .where(0)
                .equalTo(0)
                .with(new RepeatedExpansionNormalizer()) // repeated vertices with a lower score than original will have score set to -1
                .name("DeltaIteration positiveHops RepeatedExpansionNormalizer");


        // nextWorkset contains new vertices included in the expansion and old vertices whose propagated expansion hop value is greater than the original value in the current workset.
        final DataSet<Tuple2<Long, Long>> nextWorkset = outVertices // outVertices has no repeated keys
                .union(repeatedExpansionVertices) // repeatedExpansionVertices has no repeated keys
                .groupBy(0)
                .reduce(new RepeatedVertexReducer())  // if a group of 2 repeated vertices has a vertex with score -1L, we retain the -1L score
                .filter(new KeepRanksAbove(-1L)) //remove negative scores.
                .name("DeltaIteration outVertices RepeatedVertexReducer");


        final DataSet<Tuple2<Long, Long>> delta = nextWorkset
                .filter(new FilterFunction<Tuple2<Long, Long>>() { // include the newly-reached vertices with hop count = 0
                    @Override
                    public boolean filter(Tuple2<Long, Long> longLongTuple2) {
                        return longLongTuple2.f1 == 0;
                    }
                })
                .union(deltaIteration.getWorkset())
                .name("DeltaIteration nextWorkSet -> delta");

        final DataSet<Tuple2<Long, Long>> deltaIterationResult =
                deltaIteration.closeWith(delta, nextWorkset);

        return deltaIterationResult
                .map(new VertexIdExtractor())
                .name("DeltaIteration result VertexIdExtractor");
    }
/*
    public DataSet<Tuple3<Long, Double, Long>> getKHotVertices(final DataSet<Tuple2<Long, GraphUpdateTracker.UpdateInfo>> infos, final Double r, final EdgeDirection direction) {
        return infos
                .filter(new FilterVertexDegreeChange(direction, r))
                .map(new MapVertexDegreeChange(direction, r));
    }
*/
    public DataSet<Tuple2<Long, Long>> expandKHotVertices(DataSet<Tuple3<Long, Double, Long>> kHotVertices, DataSet<Tuple2<Long, Double>> previousRanks, Integer n, Double delta, double avgPrevDegree) {

        // kHotVertices is Tuple3<VERTEX ID, DEGREE UPDATE RATIO, PREVIOUS IN DEGREE>

        return kHotVertices
                .leftOuterJoin(previousRanks)
                .where(0).equalTo(0)
                .with(new JoinFunction<Tuple3<Long,Double,Long>, Tuple2<Long, Double>, Tuple2<Tuple3<Long, Double, Long>, Tuple2<Long, Double>>>() {
                    @Override
                    public Tuple2<Tuple3<Long, Double, Long>, Tuple2<Long, Double>> join(
                            Tuple3<Long, Double, Long> f0,
                            Tuple2<Long, Double> f1) {
                        if(f1 == null) {
                            // If the vertex coming from kHotVertices had no match for the leftOuterJoin with previousRanks (had no previous known rank), set the second tuple here as null.
                            return Tuple2.of(f0, null);
                        }
                        else {
                            return Tuple2.of(f0, f1);
                        }
                    }
                })
           //     .withForwardedFieldsFirst("f0.*->f0.*")
                .map(new ExpansionMapper(n, delta, avgPrevDegree));
    }

    public double getPreviousAvgInDegree(DataSet<Tuple2<Long, GraphUpdateTracker.UpdateInfo>> infos) throws Exception {
        double avgPrevDegree = infos
                .map(new MapUpdateTupleToDouble(EdgeDirection.IN))
                .reduce(new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long value1, Long value2) {
                        return value1 + value2;
                    }

                })
                .collect().get(0);
        avgPrevDegree /= infos.count();
        return avgPrevDegree;
    }


    public DataSet<Long> expand(ExecutionEnvironment env, Graph<Long, NullValue, NullValue> graph, DataSet<Tuple2<Long, GraphUpdateTracker.UpdateInfo>> infoDataSet, final DataSet<Tuple2<Long, Double>> previousResults, final EdgeDirection direction) throws Exception {

        // Make a DataSet with the vertices and their degree change ratio (degrees changed at least r in the specified direction).
        // Vertices with a previous degree of zero (0) will have Double.POSITIVE_INFINITY as degree change ratio.
        // returns Tuple3<VertexID, PageRank, InDegree>
        //final DataSet<Tuple3<Long, Double, Long>> kHotVertices = getKHotVertices(infoDataSet, this.updateRatioThreshold, direction);

        final DataSet<Tuple3<Long, Double, Long>> kHotVertices = infoDataSet
                .filter(new FilterVertexDegreeChange(direction, this.updateRatioThreshold))
                .map(new MapVertexDegreeChange(direction, this.updateRatioThreshold));

        if(this.dumpingModel) {
            kHotVertices.writeAsCsv(this.modelDirectory + "/kHotVertices_" + this.iteration + ".csv", "\n", "\t", FileSystem.WriteMode.OVERWRITE);
        }

        final double avgPrevDegree = getPreviousAvgInDegree(infoDataSet);

        //System.out.println("expand() - average degree: " + avgPrevDegree);

        final DataSet<Tuple2<Long, Long>> expandedIds = expandKHotVertices(kHotVertices, previousResults, this.neighborhoodSize, this.delta, avgPrevDegree);
        if(this.dumpingModel) {
            expandedIds.writeAsCsv(this.modelDirectory + "/expandedIds_" + this.iteration + ".csv", "\n", "\t", FileSystem.WriteMode.OVERWRITE);
        }

        // Calculating the delta iteration limit will trigger a Flink job.
        this.deltaExpansionLimit = expandedIds.max(1).collect().get(0).f1;

        // Initialize paramSetup to be the time taken to calculate the delta iteration limit.
        this.paramSetup = env.getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS);


        //System.out.println("Delta iteration limit: " + this.deltaExpansionLimit);


        final DataSet<Long> expandedVertices = deltaExpansion(expandedIds, graph, this.deltaExpansionLimit.intValue());

        if(this.dumpingModel) {
            expandedVertices
                    .map(new MapFunction<Long, Tuple1<Long>>() {

                        @Override
                        public Tuple1<Long> map(Long aLong) throws Exception {
                            return Tuple1.of(aLong);
                        }
                    })
                    .writeAsCsv(this.modelDirectory + "/expandedVertices_" + this.iteration + ".csv", "\n", "\t", FileSystem.WriteMode.OVERWRITE);
        }

        return expandedVertices;
    }

    @FunctionAnnotation.NonForwardedFields("*")
    public static class BigGraphVertexInitializer implements MapFunction<Vertex<Long, NullValue>, Double> {

        @Override
        public Double map(final Vertex<Long, NullValue> longVVVertex) {
            return 0.0d;
        }
    }

    @FunctionAnnotation.NonForwardedFields("*")
    public static class BigGraphEdgeInitializer implements MapFunction<Edge<Long, NullValue>, Double> {

        @Override
        public Double map(final Edge<Long, NullValue> longEVEdge) {
            return 0.0d;
        }
    }

    public Graph<Long, Double, Double> summaryGraph(
    		final DataSet<Long> vertexIds,
            final DataSet<Tuple2<Long, Double>> previousRanks,
            final Graph<Long, NullValue, NullValue> originalGraph) {

        // Generate vertexIds with previous rank, or initialRank (for new vertexIds), as values
    	final DataSet<Vertex<Long, Double>> kernelVertices = vertexIds
                .leftOuterJoin(previousRanks)
                .where(this.keySelector).equalTo(0)
                .with(new KernelVertexJoinFunction(this.initialRank))
                .name("Kernel vertices");
    	
    	final Graph<Long, Double, Double> doubleGraph = originalGraph
                .mapVertices(new BigGraphVertexInitializer())
                .mapEdges(new BigGraphEdgeInitializer());

        // Select the edges between the kernel vertexIds, with 1/(degree of source) as the value
    	// Select all edges whose source and target are part of kernelVertices
    	final DataSet<Edge<Long, Double>> selectedEdges = GraphUtils.selectEdges(doubleGraph, kernelVertices);

    	// Get outdegrees of the normal graph.
    	final DataSet<Tuple2<Long, LongValue>> outDegrees = originalGraph.outDegrees();
    	
    	// Get edges of the hot set whose source has outdegree > 0 and calculate their score as 1 / outdegree
        // PROBLEM LINE, GOES IDLE (tested in April/May 2018 in Chile - Flink regression bug)
    	final DataSet<Edge<Long, Double>> internalEdges = selectedEdges
                .join(outDegrees)
                .where(0).equalTo(0)
                .with(new JoinFunction<Edge<Long, Double>, Tuple2<Long, LongValue>, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> join(Edge<Long, Double> edge, Tuple2<Long, LongValue> degree) {
                        return new Edge<Long, Double>(edge.f0, edge.f1, 1.0 / degree.f1.getValue());
                    }
                })
                /*.with((edge, degree) -> {
                    assert degree.f1.getValue() > 0; //since there is an edge, out degree of edge source must be at least 1
//                    edge.setValue(1.0 / degree.f1.getValue());
                    //return edge;
                    return new Edge<Long, Double>(edge.f0, edge.f1, 1.0 / degree.f1.getValue());
                }) */
                .withForwardedFieldsFirst("f0->f0; f1->f1");
                //.returns(edgeTypeInfo);

        this.internalEdgeCount = (new AbstractID()).toString();
        internalEdges.output(new Utils.CountHelper(this.internalEdgeCount)).name(RandomWalkStatisticKeys.INTERNAL_EDGE_COUNT.toString());

        // Select all the other edges, converted to the correct type.
    	// These are edges whose source and target do not belong to the hot vertex set.
        if(this.dumpingModel) {
            internalEdges.writeAsCsv(this.modelDirectory + "/internalEdges_" + this.iteration + ".csv", "\n", "\t", FileSystem.WriteMode.OVERWRITE);

            doubleGraph.getEdges().writeAsCsv(this.modelDirectory + "/doubleGraph_" + this.iteration + ".csv", "\n", "\t", FileSystem.WriteMode.OVERWRITE);
        }


    	final DataSet<Edge<Long, Double>> externalEdges = GraphUtils.externalEdges(doubleGraph, internalEdges);
        this.externalEdgeCount = (new AbstractID()).toString();
        externalEdges.output(new Utils.CountHelper(this.externalEdgeCount)).name(RandomWalkStatisticKeys.EXTERNAL_EDGE_COUNT.toString());
    	
        // Calculate the ranks to be sent by the big vertex.
    	final DataSet<Tuple2<Long, Double>> ranksToSend = previousRanks.join(outDegrees)
                .where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Double>, Tuple2<Long, LongValue>, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> join(Tuple2<Long, Double> rank, Tuple2<Long, LongValue> degree) throws Exception {
                        return degree.f1.getValue() > 0 ?
                                Tuple2.of(rank.f0, rank.f1 / degree.f1.getValue()) :
                                Tuple2.of(rank.f0, 0.0);
                    }
                })
                /*.with((rank, degree) ->
                		degree.f1.getValue() > 0 ?
                        Tuple2.of(rank.f0, rank.f1 / degree.f1.getValue()) : 
                        Tuple2.of(rank.f0, 0.0)) */
                .withForwardedFieldsFirst("f0->f0");
                //.returns(tuple2TypeInfo);

        this.ranksToSendCount = (new AbstractID()).toString();
        ranksToSend.output(new Utils.CountHelper(this.ranksToSendCount)).name(RandomWalkStatisticKeys.VALUES_TO_SEND.toString());

        // For each edge, the rank sent is (rank of original vertex)/(out degree of original vertex).
    	// edgesToInside holds the out-edges of the bigVertex.
    	// edgesToInside as in "edges whose target is inside the hot vertex set".
    	final DataSet<Edge<Long, Double>> edgesToInside = externalEdges
                .join(kernelVertices)
                .where(1).equalTo(0) // get edges whose target is in the kernelVertices hot set, and the source is not, meaning the source is part of the big vertex
                .with((e, v) -> e)
                .returns(edgeTypeInfo)
                .join(ranksToSend)
                .where(0).equalTo(0)
                .with(new JoinFunction<Edge<Long, Double>, Tuple2<Long, Double>, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> join(Edge<Long, Double> edge, Tuple2<Long, Double> rank) throws Exception {
                        return new Edge<Long, Double>(-1L, edge.f1, rank.f1);
                    }
                })
                /*
                .with((edge, rank) -> { // for each edge whose source is in the big vertex
                   // edge.setSource(-1L); // its source id will be that of the big vertex
                  //  edge.setValue(rank.f1); // and the edge value is the previously calculated rank to send.
                    //return edge;
                    return new Edge<Long, Double>(-1L, edge.f1, rank.f1);
                })
                */
                .withForwardedFieldsFirst("f1->f1")
                .withForwardedFieldsSecond("f1->f2")
                //.returns(edgeTypeInfo)
                .groupBy(0, 1) // if we have (A->B), (C->B) and A and B are part of big vertex, then (bigVertex->B) = (A->B)+(C->B)
                .aggregate(Aggregations.SUM, 2);

        this.edgesToInsideCount = (new AbstractID()).toString();
        edgesToInside.output(new Utils.CountHelper(this.edgesToInsideCount)).name(RandomWalkStatisticKeys.EDGES_TO_INSIDE.toString());
    	
        // Add the big vertex to the set
        final Vertex<Long, Double> bigVertex = new Vertex(-1L, 1.0d);
        final DataSet<Vertex<Long, Double>> vertices = kernelVertices.union(originalGraph.getContext().fromElements(bigVertex));

        // Build the edge set
        final DataSet<Edge<Long, Double>> edges = internalEdges.union(edgesToInside);

        this.currentGraphRepresentation = Graph.fromDataSet(vertices, edges, originalGraph.getContext());

        return this.currentGraphRepresentation;
    }


    @Override
    public void initStatistics(final String statisticsDirectory) {

        // Add model-specific statistics.

        //super.statisticsMap.put(GraphStreamHandler.StatisticKeys.EXECUTION_COUNTER.toString(), new ArrayList<>());
        //super.statisticsMap.put(RandomWalkStatisticKeys.ITERATION_COUNT.toString(), new ArrayList<>());

        super.statisticsMap.put(RandomWalkStatisticKeys.EXECUTION_COUNTER.toString(), new ArrayList<>());
        super.statisticsMap.put(RandomWalkStatisticKeys.SUMMARY_VERTEX_COUNT.toString(), new ArrayList<>());
        super.statisticsMap.put(RandomWalkStatisticKeys.SUMMARY_EDGE_COUNT.toString(), new ArrayList<>());
        super.statisticsMap.put(RandomWalkStatisticKeys.INTERNAL_EDGE_COUNT.toString(), new ArrayList<>());
        super.statisticsMap.put(RandomWalkStatisticKeys.EXTERNAL_EDGE_COUNT.toString(), new ArrayList<>());
        super.statisticsMap.put(RandomWalkStatisticKeys.VALUES_TO_SEND.toString(), new ArrayList<>());
        super.statisticsMap.put(RandomWalkStatisticKeys.EDGES_TO_INSIDE.toString(), new ArrayList<>());
        super.statisticsMap.put(RandomWalkStatisticKeys.EXPANSION_LENGTH.toString(), new ArrayList<>());
        super.statisticsMap.put(RandomWalkStatisticKeys.PARAM_SETUP.toString(), new ArrayList<>());

        // Create the statistics file for this model.
        super.initStatistics(statisticsDirectory);
    }

    @Override
    public void registerStatistics(final Long iteration, final ExecutionEnvironment env) throws Exception {

        //super.statisticsMap.get(RandomWalkStatisticKeys.ITERATION_COUNT.toString()).add(iteration);

        super.statisticsMap.get(GraphStreamHandler.StatisticKeys.EXECUTION_COUNTER.toString()).add(iteration);

        if(this.currentGraphRepresentation != null) {
            super.statisticsMap.get(RandomWalkStatisticKeys.SUMMARY_VERTEX_COUNT.toString()).add(this.currentGraphRepresentation.numberOfVertices());
            super.statisticsMap.get(RandomWalkStatisticKeys.SUMMARY_EDGE_COUNT.toString()).add(this.currentGraphRepresentation.numberOfEdges());
            super.statisticsMap.get(RandomWalkStatisticKeys.EXPANSION_LENGTH.toString()).add(this.deltaExpansionLimit);
            super.statisticsMap.get(RandomWalkStatisticKeys.PARAM_SETUP.toString()).add(this.paramSetup);

            // Print all statistics of this iteration as a line in the model's statistics file.
            final StringJoiner statJoiner = new StringJoiner(";");

            int last_pos = super.statisticsMap.get(GraphStreamHandler.StatisticKeys.EXECUTION_COUNTER.toString()).size() - 1;
            String statVal = super.statisticsMap.get(GraphStreamHandler.StatisticKeys.EXECUTION_COUNTER.toString()).get(last_pos).toString();
            statJoiner.add(statVal);

            for(final String stat: super.statisticsMap.keySet()) {

                if(stat.equalsIgnoreCase(GraphStreamHandler.StatisticKeys.EXECUTION_COUNTER.toString())) {
                    continue;
                }

                last_pos = super.statisticsMap.get(stat).size() - 1;
                statVal = super.statisticsMap.get(stat).get(last_pos).toString();
                statJoiner.add(statVal);
            }
            final String statLine = statJoiner.toString();
            super.printStream.println(statLine);
            super.printStream.flush();
        }

        this.currentGraphRepresentation = null;
    }

    @Override
    public String toString() {
        final StringJoiner nameJoiner = new StringJoiner("_")
                .add("model")
                .add(String.format("%02.2f", updateRatioThreshold).replace(',','.'))
                .add(neighborhoodSize.toString())
                .add(String.format("%02.2f", delta).replace(',','.'));

        return nameJoiner.toString();
    }

    // TODO: think about centralizing all model enums in a single structure in AbstractGraphModel.java - this could avoid mismatches between enum key values
    public enum RandomWalkStatisticKeys {
        EXECUTION_COUNTER("execution_count"),
        SUMMARY_VERTEX_COUNT("summary_vertex_num"),
        SUMMARY_EDGE_COUNT("summary_edge_num"),
        INTERNAL_EDGE_COUNT("internal_edge_count"),
        EXTERNAL_EDGE_COUNT("external_edge_count"),
        VALUES_TO_SEND("values_to_send"),
        EDGES_TO_INSIDE("edges_to_inside"),
        EXPANSION_LENGTH("expansion_length"),
        ITERATION_COUNT("iteration_count"),
        PARAM_SETUP("param_setup");


        private final String text;

        /**
         * @param text String value to assign as the enum's text.
         */
        RandomWalkStatisticKeys(final String text) {
            this.text = text;
        }

        /* (non-Javadoc)
         * @see java.lang.Enum#toString()
         */
        @Override
        public String toString() {
            return text;
        }
    }

    @FunctionAnnotation.ReadFields("f1; f1.f1")
    //@FunctionAnnotation.ForwardedFieldsFirst("*->f0")
    private static class KernelVertexJoinFunction extends RichJoinFunction<Long, Tuple2<Long, Double>, Vertex<Long, Double>> {
		double initRank;
        private final LongCounter vertexCounter = new LongCounter();

        private KernelVertexJoinFunction(double initRank) {
            this.initRank = initRank;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("vertex-counter", this.vertexCounter);
        }

        @Override
        public Vertex<Long, Double> join(
                Long id,
                Tuple2<Long, Double> rank) {
            this.vertexCounter.add(1);
            return new Vertex<>(id, rank != null ? rank.f1 : this.initRank);
        }
    }

    private static class VertexKeySelector<Long> implements KeySelector<Long, Long>, ResultTypeQueryable<Long> {
		TypeInformation<Long> type;


        private VertexKeySelector(final TypeInformation<Long> type) {
            this.type = type;
        }

        @Override
        public Long getKey(final Long value) {
            return value;
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return this.type;
        }
    }

//    @FunctionAnnotation.ForwardedFields("f0")
    //@FunctionAnnotation.NonForwardedFields("f0.f1")
    public static class VertexIdExtractor implements MapFunction<Tuple2<Long, Long>, Long> {

        @Override
        public Long map(Tuple2<Long, Long> value) {
            return value.f0;
        }
    }



    //@FunctionAnnotation.ReadFields("f0.f1; f1.f1")
    public static class RepeatedExpansionNormalizer implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

        @Override
        public void join(
                Tuple2<Long, Long> f0,
                Tuple2<Long, Long> f1,
                Collector<Tuple2<Long, Long>> out) {
            if(f1.f1 > f0.f1) {
                out.collect(f1);
            }
            else {
                out.collect(Tuple2.of(f1.f0, -1L));
            }
        }
    }

    //@FunctionAnnotation.ReadFields("f0.f1; f1.f1")
//    @FunctionAnnotation.NonForwardedFieldsFirst("f0")
//    @FunctionAnnotation.NonForwardedFieldsSecond("f1")
    public static class RepeatedVertexReducer implements ReduceFunction<Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> f0, Tuple2<Long, Long> f1) {

            if(f0.f1 == -1L)
                return f0;
            else if (f1.f1 == -1L)
                return f1;
            else
                return f0.f1 > f1.f1 ? f0 : f1;
        }
    }

    //@FunctionAnnotation.ReadFields("f0.f1; f1.f1")
    public static class HopNormalizer implements ReduceFunction<Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> f0, Tuple2<Long, Long> f1) {
            return f0.f1 > f1.f1 ? f0 : f1;
        }
    }

    @FunctionAnnotation.ReadFields("f1")
    public static class KeepRanksAbove implements FilterFunction<Tuple2<Long, Long>> {
        private Long limit;

        KeepRanksAbove(Long limit) {
            this.limit = limit;
        }

        @Override
        public boolean filter(Tuple2<Long, Long> value) {
            return value.f1 > this.limit;
        }
    }

    @FunctionAnnotation.ReadFields("f1")
    private static class FilterVertexDegreeChange implements FilterFunction<Tuple2<Long,GraphUpdateTracker.UpdateInfo>> {

        private EdgeDirection direction;
        private Double r;

        FilterVertexDegreeChange(final EdgeDirection direction, final Double r) {
            this.direction = direction;
            this.r = r;
        }

        @Override
        public boolean filter(Tuple2<Long, GraphUpdateTracker.UpdateInfo> value) {
            return checkVertexDirectionDegreeRatio(r, direction, value.f1);
        }
    }

    @FunctionAnnotation.ForwardedFields("f0->f0")
    @FunctionAnnotation.ReadFields("f1")
    private static class MapVertexDegreeChange implements MapFunction<Tuple2<Long,GraphUpdateTracker.UpdateInfo>,Tuple3<Long, Double, Long>> {

        private EdgeDirection direction;
        private Double r;

        MapVertexDegreeChange(final EdgeDirection direction, final Double r) {
            this.direction = direction;
            this.r = r;
        }

        @Override
        public Tuple3<Long, Double, Long> map(Tuple2<Long, GraphUpdateTracker.UpdateInfo> value) {
            final GraphUpdateTracker.UpdateInfo u = value.f1;
            switch (direction) {
                case IN:
                    return Tuple3.of(value.f0, GraphUpdateTracker.degreeUpdateRatio(u.prevInDegree, u.currInDegree), u.currInDegree);
                case OUT:
                    return Tuple3.of(value.f0, GraphUpdateTracker.degreeUpdateRatio(u.prevOutDegree, u.currOutDegree), u.currOutDegree);
                default:
                    // TODO: this case should be removed.. It makes no sense.
                    return GraphUpdateTracker.degreeUpdateRatio(u.prevInDegree, u.currInDegree) > r ?
                            Tuple3.of(value.f0, GraphUpdateTracker.degreeUpdateRatio(u.prevInDegree, u.currInDegree), u.currInDegree) :
                            Tuple3.of(value.f0, GraphUpdateTracker.degreeUpdateRatio(u.prevOutDegree, u.currOutDegree), u.currOutDegree);
            }
        }
    }

    //@FunctionAnnotation.ReadFields("f0.f1; f0.f2; f1; f1.f1")
    @FunctionAnnotation.ForwardedFields("f0.f0->f0")
    public static class ExpansionMapper implements MapFunction<Tuple2<Tuple3<Long, Double, Long>, Tuple2<Long, Double>>, Tuple2<Long, Long>> {

        private final Integer n;
        private final double avgPrevDegree;
        private final double delta;

        public ExpansionMapper(Integer n, double delta, double avgPrevDegree) {
            this.n = n;
            this.delta = delta;
            this.avgPrevDegree = avgPrevDegree;
        }

        @Override
        public Tuple2<Long, Long> map(
                    Tuple2<
                        Tuple3<Long, Double, Long>,
                        Tuple2<Long, Double>> value) {
            Tuple3<Long, Double, Long> first = value.f0;
            Tuple2<Long, Double> second = value.f1;

//            if(first.f1.isInfinite()) {
            if(first.f1.isInfinite() || first.f2 == 0) {
                return Tuple2.of(first.f0, n.longValue());
            }
            else if(second == null) {
               // System.out.println("java - ExpansionMapper - second tuple was null, first is: " + first.toString());
                // TODO: Under what circumstances do we have ! first.f1 .isInfinite() && second == null? Find out and change code accordingly
                // if it was already
                return Tuple2.of(first.f0, n.longValue());
            }
            else {
                // If the second tuple is not null, then it means this node had a rank from the previous execution.
                final long targetDegree = first.f2;
                final double nodeRank = second.f1;
                final double logArg = avgPrevDegree * (nodeRank / targetDegree) / delta;
                final long node_level = n + Math.round(Math.log(logArg) / Math.log(avgPrevDegree));
                return Tuple2.of(first.f0, node_level < 0 ? 0 : node_level);
            }
        }
    }

    //@FunctionAnnotation.NonForwardedFields("f0")
    private static class MapUpdateTupleToDouble implements MapFunction<Tuple2<Long, GraphUpdateTracker.UpdateInfo>, Long> {
        private EdgeDirection direction;

        MapUpdateTupleToDouble(EdgeDirection direction) {
            this.direction = direction;
        }

        @Override
        public Long map(Tuple2<Long, GraphUpdateTracker.UpdateInfo> value) {
            switch (direction) {
                case IN:
                    return value.f1.getPrevInDegree();

                case OUT:
                    return value.f1.getPrevOutDegree();
                default:
                    return value.f1.getPrevInDegree();
            }
        }
    }
}
