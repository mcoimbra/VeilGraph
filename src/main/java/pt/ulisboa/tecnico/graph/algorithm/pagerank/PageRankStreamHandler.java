package pt.ulisboa.tecnico.graph.algorithm.pagerank;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import pt.ulisboa.tecnico.graph.model.GraphModelFactory;
import pt.ulisboa.tecnico.graph.model.randomwalk.BigVertexGraph;
import pt.ulisboa.tecnico.graph.model.randomwalk.BigVertexParameterHelper;
import pt.ulisboa.tecnico.graph.model.randomwalk.VertexCentricAlgorithm;
import pt.ulisboa.tecnico.graph.stream.*;
import pt.ulisboa.tecnico.graph.stream.GraphUpdateTracker.UpdateInfo;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 *
 * @author Miguel E. Coimbra
 */
@SuppressWarnings("serial")
public class PageRankStreamHandler extends GraphStreamHandler<Tuple2<Long, Double>> {

    private BigVertexGraph concreteModel;
    private DataSet<Tuple2<Long, Double>> previousRanks;

    private TypeSerializerInputFormat<Tuple2<Long, Double>> rankInputFormat;
    private TypeSerializerOutputFormat<Tuple2<Long, Double>> rankOutputFormat;
    private TypeInformation<Tuple2<Long, Double>> rankTypeInfo;

    final private Integer pageRankIterations;
    final private Integer pageRankSize;
    final private Double pageRankDampeningFactor;


    @Override
    protected String getCustomName() {
        return super.customName;
    }



    private boolean isRunningSummarizedMethod() {
        return argValues.containsKey(BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM.toString()) &&
                argValues.containsKey(BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM.toString()) &&
                argValues.containsKey(BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM.toString());
    }


    public PageRankStreamHandler(final Map<String, Object> argValues, final Function<String, Long> f) {
        this(Action.COMPUTE_EXACT, argValues, f);
    }



    public PageRankStreamHandler(GraphStreamHandler.Action executionStrategy, final Map<String, Object> argValues, final Function<String, Long> f) {
        super(executionStrategy, argValues, f, "pagerank");
        super.algorithmName = "pagerank";

        this.pageRankIterations = ((Integer)argValues.get(PageRankParameterHelper.PageRankArgumentName.PAGERANK_ITERATIONS.toString()));
        this.pageRankSize = ((Integer)argValues.get(PageRankParameterHelper.PageRankArgumentName.PAGERANK_SIZE.toString()));
        this.pageRankDampeningFactor = ((Double)argValues.get(PageRankParameterHelper.PageRankArgumentName.DAMPENING_FACTOR.toString()));

        final StringJoiner nameJoiner = new StringJoiner("_")
                .add(super.datasetName.replace(".tsv", ""))
                .add(this.pageRankIterations.toString())
                .add(this.pageRankSize.toString())
                .add(String.format("%02.2f", this.pageRankDampeningFactor).replace(',','.'));

        if(this.isRunningSummarizedMethod()) {

            // Configure the big vertex representation.
            final GraphModelFactory<NullValue, NullValue> gmf = new GraphModelFactory<>();
            super.model = gmf.getGraphModel(GraphModelFactory.Model.BIG_VERTEX, super.argValues, super.modelDirectory);
            this.concreteModel = (BigVertexGraph) super.model;


            final TypeInformation<Long> keySelector = TypeInformation.of(new TypeHint<Long>(){});
            final TypeInformation<Edge<Long, Double>> edgeTypeInfo = TypeInformation.of(new TypeHint<Edge<Long, Double>>() {});
            final TypeInformation<Tuple2<Long, Double>> tuple2TypeInfo = TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {});

            this.concreteModel.setTypeInfo(keySelector, edgeTypeInfo, tuple2TypeInfo);

            nameJoiner.add(super.model.toString());
        }
        else {
            nameJoiner.add("complete");
        }

        super.customName = nameJoiner.toString();
    }



    @Override
    public void init() throws Exception {

        this.outputFormat = new PageRankCsvOutputFormat(super.resultsDirectory, System.lineSeparator(), ";", true, true);



        if (! this.isRunningSummarizedMethod()) {
            this.outputFormat.setName("complete_PR");
            System.out.println("Running complete version of PageRank.");
        }
        else {
            this.outputFormat.setName("summary_PR");

            System.out.println("Running summarized version of PageRank.");
        }

        // Generic PageRank statistic.
        super.statisticsMap.put(BigVertexGraph.RandomWalkStatisticKeys.ITERATION_COUNT.toString(), new ArrayList<>());



        // Trigger the first PageRank execution.
        final TypeInformation<Tuple2<Long, Long>> edgeTypeInfo = super.graph.getEdgeIds().getType();
        super.edgeInputFormat = new TypeSerializerInputFormat<>(edgeTypeInfo);

        super.edgeOutputFormat = new TypeSerializerOutputFormat<>();
        super.edgeOutputFormat.setInputType(edgeTypeInfo, super.env.getConfig());
        super.edgeOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        super.edgeOutputFormat.setOutputFilePath(new Path(super.cacheDirectory + "/edges" + super.iteration));

        super.graph.getEdgeIds().output(super.edgeOutputFormat);

        final String spillJobName = "GraphBolt initial edge disk spill";
        final JobExecutionResult diskSpillJob = super.env.execute(spillJobName);
        System.out.println(String.format("Flink Job: %s, %d.%d",
                spillJobName,
                diskSpillJob.getNetRuntime(TimeUnit.SECONDS),
                diskSpillJob.getNetRuntime(TimeUnit.MILLISECONDS) % 1000));

        final DataSet<Tuple2<Long, Double>> ranks = super.graph.
                run(new SimplePageRank<>(this.pageRankDampeningFactor, 1.0, this.pageRankIterations));

        this.rankTypeInfo = ranks.getType();
        this.rankInputFormat = new TypeSerializerInputFormat<>(rankTypeInfo);
        this.rankOutputFormat = new TypeSerializerOutputFormat<>();
        this.rankOutputFormat.setInputType(this.rankTypeInfo, super.env.getConfig());
        this.rankOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        this.rankOutputFormat.setOutputFilePath(new Path(super.cacheDirectory + "/ranks" + super.iteration));
        ranks.output(this.rankOutputFormat);

        outputResult("", ranks);
        
        // Trigger the Flink execution for the initial PageRank calculation, top rank spill and (optionally) all of the ranks to disk cache. 
        final String initialAppExecution = "Initial application (PageRank) execution";
        super.env.execute(initialAppExecution);

        super.graphUpdateTracker.resetAll();


        // Add the random walk iterations to the GraphStreamHandler statistics file.
        super.statOrder.add(BigVertexGraph.RandomWalkStatisticKeys.ITERATION_COUNT.toString());
        super.statisticsMap.put(BigVertexGraph.RandomWalkStatisticKeys.ITERATION_COUNT.toString(), new ArrayList<>());
    }

    public class RankMergeOperator extends RichCoGroupFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

        @Override
        public void coGroup(Iterable<Tuple2<Long, Double>> previous, Iterable<Tuple2<Long, Double>> newRanks,
                            Collector<Tuple2<Long, Double>> out)  {
            Iterator<Tuple2<Long, Double>> prevIt = previous.iterator();
            Iterator<Tuple2<Long, Double>> newIt = newRanks.iterator();

            if (newIt.hasNext()) {
                Tuple2<Long, Double> next = newIt.next();
                // Avoid the big-vertex summarization construct which was generated by this.summaryGraph.run().
                out.collect(next);
            } else if (prevIt.hasNext()) {
                out.collect(prevIt.next());
            }
        }
    }


    private void outputResult(final String date, final DataSet<Tuple2<Long, Double>> ranks) {
        super.outputFormat.setIteration(super.iteration);
        super.outputFormat.setTags(date);
        ranks
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1).first(this.pageRankSize)
                .output(super.outputFormat);
    }

    // GraphBolt stream UDFs.


    @Override
    protected boolean beforeUpdates(final GraphUpdates<Long, NullValue> updates, final GraphUpdateStatistics statistics) {
        //TODO: logic - beforeUpdates(final GraphUpdates<Long, NullValue> updates, final GraphUpdateStatistics statistics)
	    return true;
    }

    @Override
    protected GraphStreamHandler.Action onQuery(final Long id,
                                                final String query,
                                                final Graph<Long, NullValue, NullValue> graph,
                                                final GraphUpdates<Long, NullValue> updates,
                                                final GraphUpdateStatistics statistics,
                                                final Map<Long, GraphUpdateTracker.UpdateInfo> updateInfos) {


        this.rankInputFormat.setFilePath(this.cacheDirectory + "/ranks" + ((iteration - 1) % super.storedIterations));
        this.previousRanks = super.env.createInput(this.rankInputFormat, rankTypeInfo).name("PageRank - read previous ranks.");

        if(this.concreteModel != null) {
            this.concreteModel.updateGraph(super.timeToSnapshot(), super.iteration);
            this.concreteModel.setModelDirectory(super.modelDirectory);
            return Action.COMPUTE_APPROXIMATE;
        }
        else
            return Action.COMPUTE_EXACT;

    }
    @Override
    protected void onQueryResult(
            final Long id,
            final String query,
            final GraphStreamHandler.Action action,
            final Graph<Long, NullValue, NullValue> graph) {




    }



    //TODO: executeExact and executeApproximate should be moved to GraphModel interface. Need to see what that change would imply wrt GraphBolt's UDF time measurements inside run()

    // GraphBolt execution strategy UDFs.
    @Override
    protected Long executeExact() throws Exception {

        final DataSet<Tuple2<Long, Double>> newRanks =
                super.graph.run(new SimplePageRank<>(this.pageRankDampeningFactor, this.previousRanks, this.pageRankIterations));

        outputResult("", newRanks);
        this.rankOutputFormat.setOutputFilePath(new Path(super.cacheDirectory + "/ranks" + (super.iteration % super.storedIterations) ));
        newRanks.output(this.rankOutputFormat).name("Complete PageRank - execution and results");

        super.env.execute("PageRankStreamHandler - executeExact");

        final long pageRankTime = super.env.getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS);

        super.statisticsMap.get(BigVertexGraph.RandomWalkStatisticKeys.ITERATION_COUNT.toString()).add(this.pageRankIterations.longValue());

        return pageRankTime;
    }


    public static class PageRankFunction implements Function<Double, Double>, Serializable {

        private final Double dampening;

        public PageRankFunction(Double dampening) {
            this.dampening = dampening;
        }


        @Override
        public Double apply(Double rankSum) {
            return (this.dampening * rankSum) + (1 - this.dampening);
        }
    }



    @Override
    protected Long executeApproximate() throws Exception {

        final DataSet<Tuple2<Long, UpdateInfo>> infoDataSet = super.graphUpdateTracker.getNewGraphInfo(super.env, super.graph);

        // Produce the summarized Flink graph instance.
        final Graph<Long, Double, Double> summaryGraph = this.concreteModel.getGraph(super.env, super.graph, infoDataSet, this.previousRanks);

        // Vertex value update to pass to the big vertex algorithm instance.
        final PageRankFunction prf = new PageRankFunction(this.pageRankDampeningFactor);
        final GraphAlgorithm<Long, Double, Double, DataSet<Tuple2<Long, Double>>> algo = new VertexCentricAlgorithm(this.pageRankIterations, prf);
        final DataSet<Tuple2<Long, Double>> ranks = summaryGraph.run(algo);

        // Merge with previous ranks.
        final DataSet<Tuple2<Long, Double>> fixedRanks = this.previousRanks.coGroup(ranks)
                .where(0).equalTo(0)
                .with(new RankMergeOperator())
                .returns(new TypeHint<Tuple2<Long, Double>>() {})
                .name("Merge with previous");


        outputResult("", fixedRanks);
        this.rankOutputFormat.setOutputFilePath(new Path(super.cacheDirectory + "/ranks" + (super.iteration % super.storedIterations)));
        fixedRanks.output(this.rankOutputFormat).name("Summary PageRank - execution and results");

        super.statisticsMap.get(BigVertexGraph.RandomWalkStatisticKeys.ITERATION_COUNT.toString()).add(this.pageRankIterations.longValue());

        // This is the time taken by a single Flink job to build the summary graph and then run PageRank.
        final JobExecutionResult jer = super.env.getLastJobExecutionResult();

        final long approxComputationTime = jer.getNetRuntime(TimeUnit.MILLISECONDS);

        return super.model.getSetupTime() + approxComputationTime;


    }

    @Override
    protected Long executeAutomatic() {
        //TODO: logic - executeAutomatic()
        return 0L;
    }
}
