package pt.ulisboa.tecnico.graph.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
//import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;


import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
//import java.util.function.Predicate;
import java.util.stream.Collectors;



/**
 * Tracker of the updates to a graph
 *
 * @param <K>
 * @param <VV>
 * @param <EV>
 * @author Renato Rosa
 */
public class GraphUpdateTracker<K, VV, EV> implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 7046134030072228371L;
	private final Set<K> verticesToAdd = new HashSet<>();
    private final Set<K> verticesToRemove = new HashSet<>();
    private final Set<Edge<K, EV>> edgesToAdd = new HashSet<>();
    private final Set<Edge<K, EV>> edgesToRemove = new HashSet<>();

    private Map<K, UpdateInfo> infoMap = new HashMap<>();

    private long currentNumberOfVertices;
    private long currentNumberOfEdges;

    // Accounts for the time user-code (non-Flink) runs to add edges.
    private long accumulatedTime;

//    private DataSet<Tuple2<Long, UpdateInfo>> degreeDataset = null;

    private DataSet<Tuple2<K, GraphUpdateTracker.UpdateInfo>> infoMapToDataSet(Map<K, UpdateInfo> infos, ExecutionEnvironment env) {
        final List<Tuple2<K, GraphUpdateTracker.UpdateInfo>> infoList = infos
                .entrySet()
                .stream()
                //.filter(predicate)
                .map(new Function<Entry<K, UpdateInfo>, Tuple2<K, UpdateInfo>>() {
                    @Override
                    public Tuple2<K, GraphUpdateTracker.UpdateInfo> apply(Map.Entry<K, UpdateInfo> t) {
                        return Tuple2.of(t.getKey(), t.getValue());
                    }
                })
                .collect(Collectors.toList());

        return env.fromCollection(infoList);
    }


    public DataSet<Tuple2<K, GraphUpdateTracker.UpdateInfo>> getNewGraphInfo(final ExecutionEnvironment env, final Graph<K, NullValue, NullValue> graph) {
        final DataSet<Tuple2<K, UpdateInfo>>  newInfos = infoMapToDataSet(Collections.unmodifiableMap(this.infoMap), env)
                .filter(new FilterFunction<Tuple2<K, GraphUpdateTracker.UpdateInfo>>() {
                    @Override
                    public boolean filter(Tuple2<K, UpdateInfo> longUpdateInfoTuple2) throws Exception {
                        return longUpdateInfoTuple2.f1.nUpdates > 0;
                    }
                });


        final DataSet<Tuple2<K, UpdateInfo>> infoDataSet =
                graph.inDegrees()
                        .join(graph.outDegrees())
                        .where(0).equalTo(0)
                        .with(new JoinFunction<Tuple2<K, LongValue>, Tuple2<K, LongValue>, Tuple2<K, UpdateInfo>>() {
                            @Override
                            public Tuple2<K, UpdateInfo> join(Tuple2<K, LongValue> inDeg, Tuple2<K, LongValue> outDeg) throws Exception {
                                // This will produce a DataSet where all UpdateInfo elements have nUpdates == 0.
                                return Tuple2.of(inDeg.f0, new UpdateInfo(inDeg.f1.getValue(), outDeg.f1.getValue()));
                            }
                        })
                        .union(newInfos)
                        .groupBy(0)
                        .reduce(new ReduceFunction<Tuple2<K, UpdateInfo>>() {
                            @Override
                            public Tuple2<K, UpdateInfo> reduce(Tuple2<K, UpdateInfo> longUpdateInfoTuple2, Tuple2<K, UpdateInfo> t1) throws Exception {
                                // The tuples with UpdateInfo.nUpdates > 0 are the actual updates received, the others with UpdateInfo.Updates == 0 were just created from the join above.
                                if(longUpdateInfoTuple2.f1.nUpdates > t1.f1.nUpdates) {
                                    longUpdateInfoTuple2.f1.currInDegree += t1.f1.currInDegree;
                                    longUpdateInfoTuple2.f1.currOutDegree += t1.f1.currOutDegree;
                                    longUpdateInfoTuple2.f1.prevInDegree = t1.f1.prevInDegree;
                                    longUpdateInfoTuple2.f1.prevOutDegree = t1.f1.prevOutDegree;
                                    return longUpdateInfoTuple2;
                                }
                                else {
                                    t1.f1.currInDegree += longUpdateInfoTuple2.f1.currInDegree;
                                    t1.f1.currOutDegree += longUpdateInfoTuple2.f1.currOutDegree;
                                    t1.f1.prevInDegree = longUpdateInfoTuple2.f1.prevInDegree;
                                    t1.f1.prevOutDegree = longUpdateInfoTuple2.f1.prevOutDegree;
                                    return t1;
                                }
                            }
                        });

        return infoDataSet;
    }

    public class InDegreeToUpdateInfoMapper implements MapFunction<Tuple2<K, LongValue>, Tuple2<K, UpdateInfo>> {

		@Override
		public Tuple2<K, UpdateInfo> map(Tuple2<K, LongValue> value) throws Exception {
			return Tuple2.of(value.f0, new UpdateInfo(value.f1.getValue(), -1));
		}
		
	}
    
    public GraphUpdateTracker(final Graph<K, VV, EV> initialGraph) {
    	System.out.println("Initializing GraphUpdateTracker.");
    	
    	//this.infoMap = new HashMap<>();
        this.accumulatedTime = 0L;
        try {
            long start = System.nanoTime();

            // NOTE: this was commented out to avoid performing "collect" over a really large graph. Code similar to this one is now invoked when the updates are to be used.
            /*
            if (ApproximatePageRank.USING_UPDATE_INFO_COLLECT) {
                List<Tuple2<K, UpdateInfo>> degrees = initialGraph.inDegrees()
                        .join(initialGraph.outDegrees())
                        .where(0).equalTo(0)
                        .with(new JoinFunction<Tuple2<K, LongValue>, Tuple2<K, LongValue>, Tuple2<K, UpdateInfo>>() {
                            @Override
                            public Tuple2<K, UpdateInfo> join(Tuple2<K, LongValue> inDeg, Tuple2<K, LongValue> outDeg) throws Exception {
                                return Tuple2.of(inDeg.f0, new UpdateInfo(inDeg.f1.getValue(), outDeg.f1.getValue()));
                            }
                        }).collect();
                this.infoMap = degrees.stream().collect(Collectors.toMap(t -> t.f0, t -> t.f1));
            } */

            // Retrieving the number of vertices and edges will trigger a Flink execution.
            this.currentNumberOfVertices = initialGraph.numberOfVertices();
            this.currentNumberOfEdges = initialGraph.numberOfEdges();

            /*
            this.accumulatedTime += (System.nanoTime() - start) / 1000;

            System.out.println(String.format("Initialized GraphUpdateTracker (%d.%d s)",
                TimeUnit.MILLISECONDS.toSeconds(this.accumulatedTime),
                    this.accumulatedTime % 1000));
            */

            this.accumulatedTime += (System.nanoTime() - start);
            System.out.println(String.format("Initialized GraphUpdateTracker (%d.%d s)",
            		TimeUnit.NANOSECONDS.toSeconds(this.accumulatedTime),
            		TimeUnit.NANOSECONDS.toMillis(this.accumulatedTime) % 1000));
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 
     * Used for basic approximate computing.
     * @author Renato Rosa
     *//*
    public static <K> Set<K> updatedAboveThresholdIds(final Map<K, GraphUpdateTracker.UpdateInfo> infoMap, final double threshold, final EdgeDirection direction) {
        
    	
    	if (threshold <= 0.0) {
            return allUpdatedIds(infoMap, direction);
        }

        
        
    	
        Predicate<Map.Entry<K, UpdateInfo>> pred;
        switch (direction) {
            case IN:
                pred = e -> {
                    GraphUpdateTracker.UpdateInfo i = e.getValue();
                    return degreeUpdateRatio(i.prevInDegree, i.currInDegree) > threshold;
                };
                break;
            case OUT:
                pred = e -> {
                    GraphUpdateTracker.UpdateInfo i = e.getValue();
                    return degreeUpdateRatio(i.prevOutDegree, i.currOutDegree) > threshold;
                };
                break;
            default:
                pred = e -> {
                    GraphUpdateTracker.UpdateInfo i = e.getValue();
                    return degreeUpdateRatio(i.prevInDegree, i.currInDegree) > threshold ||
                            degreeUpdateRatio(i.prevOutDegree, i.currOutDegree) > threshold;
                };
                break;
        }

        return infoMap.entrySet().stream()
                .filter(pred)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    */

    /**
     * Calculate the ratio between the current vertex degree and the previous one, subtract one and return the absolute value.
     * 
     * @param prevDeg previous value for vertex degree
     * @param currDeg current value for the vertex degree
     * @author Miguel E. Coimbra
     * @return the vertex degree change ratio
     */
    public static double degreeUpdateRatio(final long prevDeg, final long currDeg) {
        assert prevDeg >= 0 && currDeg >= 0 : "Negative degrees";
        if (prevDeg == 0) {
            return Double.POSITIVE_INFINITY;
        }

        return Math.abs((double) (currDeg / prevDeg) - 1.0);
    }
    


    public static <K> Set<K> allUpdatedIds(final Map<K, GraphUpdateTracker.UpdateInfo> infoMap, final EdgeDirection direction) {
        Set<K> set1 = new HashSet<>();
        Set<K> set2 = new HashSet<>();

        if (direction == EdgeDirection.IN || direction == EdgeDirection.ALL) {
            set1 = infoMap.entrySet().stream()
                    .filter(e -> e.getValue().currInDegree != e.getValue().prevInDegree)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }

        if (direction == EdgeDirection.OUT || direction == EdgeDirection.ALL) {
            set2 = infoMap.entrySet().stream()
                    .filter(e -> e.getValue().currOutDegree != e.getValue().prevOutDegree)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }

        set1.addAll(set2);
        return set1;
    }

    public GraphUpdates<K, EV> getGraphUpdates() {
        return new GraphUpdates<>(this.verticesToAdd, this.verticesToRemove, this.edgesToAdd, this.edgesToRemove);
    }
    
    public GraphUpdateStatistics getUpdateStatistics() {
        
    	return new GraphUpdateStatistics(
        		this.verticesToAdd.size(), 
        		this.verticesToRemove.size(), 
        		this.edgesToAdd.size(), 
        		this.edgesToRemove.size(),
        		-1,
                this.currentNumberOfVertices + this.verticesToAdd.size() - this.verticesToRemove.size(),
                this.currentNumberOfEdges + this.edgesToAdd.size() - this.edgesToRemove.size());
    }

    /*
    public DataSet<Tuple2<Long, UpdateInfo>> getUpdateDataset() {
    	return this.degreeDataset;
    }
    */
    public Map<K, UpdateInfo> getUpdateInfos() {
        return Collections.unmodifiableMap(this.infoMap);
    }

    public void addEdge(final Edge<K, EV> edge) {
        long start = System.nanoTime();
        this.edgesToAdd.add(edge);
        this.edgesToRemove.remove(edge);
        UpdateInfo info = putOrGetInfo(edge.getSource());
        info.nUpdates++;
        info.currOutDegree++;

        info = putOrGetInfo(edge.getTarget());
        info.nUpdates++;
        info.currInDegree++;
        this.accumulatedTime += (System.nanoTime() - start);
    }

    private UpdateInfo putOrGetInfo(final K vertex) {
        return this.infoMap.computeIfAbsent(vertex, k -> {
        	this.verticesToAdd.add(vertex);
        	this.verticesToRemove.remove(vertex);
            return new UpdateInfo(0, 0);
        });
    }
    
    void removeEdge(final Edge<K, EV> edge) {
    	this.edgesToRemove.add(edge);
    	this.edgesToAdd.remove(edge);
        if (this.infoMap.containsKey(edge.getSource())) {
            UpdateInfo info = this.infoMap.get(edge.getSource());
            info.nUpdates++;
            info.currOutDegree--;
            checkRemove(edge.getSource());
        }

        if (this.infoMap.containsKey(edge.getTarget())) {
            UpdateInfo info = this.infoMap.get(edge.getTarget());
            info.nUpdates++;
            info.currInDegree--;
            checkRemove(edge.getTarget());
        }
    }

    private void checkRemove(final K vertex) {
        UpdateInfo info = this.infoMap.get(vertex);
        if (info.currInDegree == 0 && info.currOutDegree == 0) {
        	this.verticesToRemove.add(vertex);
        	this.verticesToAdd.remove(vertex);
        	this.infoMap.remove(vertex);
        }
    }

    /*
    public void resetAccumulatedTime() {
    	this.accumulatedTime = 0;
    }
*/

    public void resetUpdates() {
    	this.verticesToAdd.clear();
    	this.verticesToRemove.clear();
    	this.edgesToAdd.clear();
    	this.edgesToRemove.clear();
    	this.accumulatedTime = 0;
    }


    
    public void reset(final Collection<K> ids) {
        ids.forEach(id -> this.infoMap.get(id).reset());
    }

    
    /*
    @ForwardedFields("f0->f0")
	private class InfoDataSetReseter implements MapFunction<Tuple2<Long,GraphUpdateTracker.UpdateInfo>,Tuple2<Long,GraphUpdateTracker.UpdateInfo>> {
		@Override
		public Tuple2<Long, UpdateInfo> map(Tuple2<Long, UpdateInfo> value) throws Exception {
			value.f1.reset();
			return value;
		}
    }
    */
    
    public void resetAll() {
    	this.infoMap.values().forEach(UpdateInfo::reset);
    	this.accumulatedTime = 0;
    }

    public long getAccumulatedTime() {
        return this.accumulatedTime;
    }
    
    

    public static class UpdateInfo implements Serializable {

		private static final long serialVersionUID = 8101862332079120102L;
		public long nUpdates;
        public long prevInDegree;
        public long currInDegree;
        public long prevOutDegree;
        public long currOutDegree;

        public long getPrevInDegree() {
        	return this.prevInDegree;
        }
        
        public long getPrevOutDegree() {
        	return this.prevOutDegree;
        }
        
        public UpdateInfo(final long inDegree, final long outDegree) {
        	this.currInDegree = this.prevInDegree = inDegree;
        	this.currOutDegree = this.prevOutDegree = outDegree;
        	this.nUpdates = 0;
        }

        void reset() {
        	this.nUpdates = 0;
        	this.prevInDegree = this.currInDegree;
        	this.prevOutDegree = this.currOutDegree;
        }
        
        @Override
        public String toString() {
    		return "nUpdates: " + nUpdates + "\tcurrInDegree: " + this.currInDegree + "\tprevInDegree: " + this.prevInDegree;
        }
    }

	
}
