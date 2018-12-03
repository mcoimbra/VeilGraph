package pt.ulisboa.tecnico.graph.util;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.typeinfo.TypeHint;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
//import org.apache.flink.graph.library.linkanalysis.PageRank.Result;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;


import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

import pt.ulisboa.tecnico.graph.stream.GraphUpdateTracker.UpdateInfo;


import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * Utility methods for graphs in Flink
 *
 * @author Renato Rosa
 */
@SuppressWarnings("serial")
public class GraphUtils {



	public static <T> TypeInformation getTypeInfo(T instance) {

		if(instance instanceof String)
			return TypeInformation.of(String.class);
		else if(instance instanceof Boolean)
			return TypeInformation.of(Boolean.class);
		else if(instance instanceof Byte)
			return TypeInformation.of(Byte.class);
		else if(instance instanceof Short)
			return TypeInformation.of(Short.class);
		else if(instance instanceof Integer)
			return TypeInformation.of(Integer.class);
		else if(instance instanceof Long)
			return TypeInformation.of(Long.class);
		else if(instance instanceof Float)
			return TypeInformation.of(Float.class);
		else if(instance instanceof Double)
			return TypeInformation.of(Double.class);
		else if(instance instanceof Character)
			return TypeInformation.of(Character.class);
		else if(instance instanceof Date)
			return TypeInformation.of(Date.class);
		else if(instance instanceof Void)
			return TypeInformation.of(Void.class);
		else if(instance instanceof BigInteger)
			return TypeInformation.of(BigInteger.class);
		else if(instance instanceof BigDecimal)
			return TypeInformation.of(BigDecimal.class);

		throw new IllegalArgumentException("Unsupported type received: " + instance);
	}


	public static void deleteFileOrFolder(final java.nio.file.Path path) throws IOException {
		Files.walkFileTree(path, new SimpleFileVisitor<Path>(){
			@Override public FileVisitResult visitFile(final java.nio.file.Path file, final BasicFileAttributes attrs)
					throws IOException {
				Files.delete(file);
				return java.nio.file.FileVisitResult.CONTINUE;
			}

			@Override public FileVisitResult visitFileFailed(final java.nio.file.Path file, final IOException e) {
				return handleException(e);
			}

			private FileVisitResult handleException(final IOException e) {
				e.printStackTrace(); // replace with more robust error handling
				return java.nio.file.FileVisitResult.TERMINATE;
			}

			@Override public FileVisitResult postVisitDirectory(final java.nio.file.Path dir, final IOException e)
					throws IOException {
				if(e!=null)return handleException(e);
				Files.delete(dir);
				return java.nio.file.FileVisitResult.CONTINUE;
			}
		});
	}

    static <T> DataSet<T> emptyDataSet(final ExecutionEnvironment env, final TypeInformation<T> typeInformation) {
        return env.fromCollection(Collections.emptyList(), typeInformation);
    }

    static <T> DataSet<T> emptyDataSet(final ExecutionEnvironment env, final TypeHint<T> typeHint) {
        return env.fromCollection(Collections.emptyList(), typeHint.getTypeInfo());
    }

    static <T> DataSet<T> emptyDataSet(final ExecutionEnvironment env, final Class<T> clazz) {
        return env.fromCollection(Collections.emptyList(), TypeInformation.of(clazz));
    }

    static <K, VV, EV> Graph<K, VV, EV> emptyGraph(final ExecutionEnvironment env, final Class<K> keyType,
    		final Class<VV> vertexType, final Class<EV> edgeType) {
    	final Objenesis instantiator = new ObjenesisStd(true);
    	final K k = instantiator.newInstance(keyType);
    	final VV vv = instantiator.newInstance(vertexType);
    	final EV ev = instantiator.newInstance(edgeType);

        TypeInformation<Vertex<K, VV>> vertexInfo = TypeExtractor.getForObject(new Vertex<>(k, vv));
        TypeInformation<Edge<K, EV>> edgeInfo = TypeExtractor.getForObject(new Edge<>(k, k, ev));

        return Graph.fromDataSet(emptyDataSet(env, vertexInfo), emptyDataSet(env, edgeInfo), env);

    }

    static <K> Graph<K, NullValue, NullValue> emptyGraph(final ExecutionEnvironment env, final Class<K> keyType) {
    	final Objenesis instantiator = new ObjenesisStd(true);
    	final K k = instantiator.newInstance(keyType);

    	final TypeInformation<Vertex<K, NullValue>> vertexInfo = TypeExtractor.getForObject(new Vertex<>(k, NullValue.getInstance()));
    	final TypeInformation<Edge<K, NullValue>> edgeInfo = TypeExtractor.getForObject(new Edge<>(k, k, NullValue.getInstance()));

        return Graph.fromDataSet(emptyDataSet(env, vertexInfo), emptyDataSet(env, edgeInfo), env);
    }

    /**
	 * Extract the key type from a PageRank.Result<K>
	 * 
	 * @see org.apache.flink.graph.library.linkanalysis.PageRank
	 * @see org.apache.flink.graph.library.linkanalysis.PageRank.Result
	 *
	 * @param <K> the key type for vertex identifiers in the Result class type.
	 *
	 * @author Miguel E. Coimbra
	 */
/*
	static class PageRankResultKeyExtractor<K> implements KeySelector<Result<K>, K> {

		@Override
		public K getKey(Result<K> value) throws Exception {
			return value.getVertexId0();
		}
	}
	*/
	/**
	 * Extract the value type from a PageRank.Result<V>
	 * 
	 * @see org.apache.flink.graph.library.linkanalysis.PageRank
	 * @see org.apache.flink.graph.library.linkanalysis.PageRank.Result
	 *
	 * @param <K> the key type for vertex identifiers in the Result class type.
	 * @param <V> the type for vertex values in the Result class type.
	 *
	 * @author Miguel E. Coimbra
	 */
	/*
	static class PageRankResultValueExtractor<K> implements KeySelector<Result<K>, Double> {

		@Override
		public Double getKey(Result<K> value) throws Exception {
			return value.getPageRankScore().getValue();
		}
	}
    
	public static class PageRankResultToDataSetMapper<K> implements MapFunction<Result<K>, Tuple2<K, Double>> {

		@Override
		public Tuple2<K, Double> map(Result<K> value) throws Exception {
			return Tuple2.of(value.getVertexId0(), value.getPageRankScore().getValue());
		}
	}
    */
    
	/**
	 * Convert the vertex type of a Gelly graph from Vertex<Long, NullValue> to Vertex<Long, Double>
	 * 
	 * @see org.apache.flink.graph.Graph
	 * @see org.apache.flink.graph.Vertex
	 *
	 * @param <K> the key type for vertex identifiers
	 * @param <VV> the value type for vertices
	 *
	 * @author Miguel E. Coimbra
	 */
	
	public static class GraphDoubleInitializer<K, VV> implements MapFunction<Vertex<K, VV>, Double> {

		@Override
		public Double map(final Vertex<K, VV> value) throws Exception {
			return new Double(+1.000000000d);
		}
	}
	
    /*
     * TODO: there is room for optimization here.
     * When we move from one level (parameter n) to another, neighbours already used in the previous level will be used again.
     */
    public static <K, VV, EV> DataSet<K> expandedVertexIds(final Graph<K, VV, EV> originalGraph, final DataSet<K> originalVertexIds, int level) throws Exception {
        DataSet<K> expandedIds = originalVertexIds;
        final VertexKeySelector<K> keySelector = new VertexKeySelector<>(originalGraph.getVertexIds().getType());
        while (level > 0) {
            DataSet<K> firstNeighbours = expandedIds
                    .join(originalGraph.getEdges())
                    .where(keySelector).equalTo(0)
                    .with((id, e) -> e.getTarget())
                    .returns(expandedIds.getType())
                    .name("Neighbours level " + level);

            expandedIds = expandedIds.union(firstNeighbours).distinct().name("Expanded level " + level);
            level--;
        }

        return expandedIds;
    }
    
    
    /**
     * MapFunction that calculates individual node's neighborhood inclusion length for approximate computing.
     * @author Miguel E. Coimbra
     *
     * @param <K>
     */
    public static class CustomNeighborhoodMapFunction<K> implements MapFunction<Tuple2<Tuple2<K, Double>, Tuple2<K, Double>>, Tuple2<K, Long>> {

		private long level;
		private double delta;
		private Map<Long, UpdateInfo> infos;
		private double avgDegree;
		
		public CustomNeighborhoodMapFunction(long level, double delta, Map<Long, UpdateInfo> infos) {
			this.level = level;
			this.delta = delta;
			this.infos = infos;
			this.avgDegree = infos.values().stream().mapToDouble(i -> i.getPrevInDegree()).average().getAsDouble(); 
		}

		@Override
		public Tuple2<K, Long> map(Tuple2<Tuple2<K, Double>, Tuple2<K, Double>> value) throws Exception {
			if(value.f0.f1.isInfinite()) {
				return Tuple2.of(value.f0.f0, level);
			}
			else {
				//TODO LV: o que interessa e o valor absoluto da diferenca.
				// commented on 2017-Oct-19
				//double logArg = level + avgDegree * (value.f1.f1 / infos.get(value.f0.f0).currInDegree) / delta;
				// Long node_level = Math.round(Math.log(logArg) / Math.log(avgDegree));
				double logArg = avgDegree * (value.f1.f1 / infos.get(value.f0.f0).currInDegree) / delta;
				Long node_level = level + Math.round(Math.log(logArg) / Math.log(avgDegree));
				return Tuple2.of(value.f0.f0, node_level < 0 ? 0 : node_level);
			}
		}
    }
    
    /**
     * Keep only vertices whose neighborhood hasn't been surpassed yet.
     * @author Miguel E. Coimbra
     *
     * @param <K>
     */
    public static class PositiveNeighborhoodFilterFunction<K> implements FilterFunction<Tuple2<K, Long>> {
		
		@Override
		public boolean filter(Tuple2<K, Long> value) throws Exception {
			return value.f1 > 0;
		}
    }
    
    /**
     * Decrement the f1 of the vertices that passed the positive value filter.
     * @author Miguel E. Coimbra
     *
     * @param <K>
     */
    public static class NeighborhoodHopMapFunction<K> implements MapFunction<Tuple2<K, Long>, Tuple2<K, Long>> {

		@Override
		public Tuple2<K, Long> map(Tuple2<K, Long> value) throws Exception {
			return Tuple2.of(value.f0, 0L);
		}
    }
    
    /**
     * Retain only the vertex ids.
     * @author Miguel E. Coimbra
     *
     * @param <K>
     */
    public static class RetainVertexIdMapFunction<K> implements MapFunction<Tuple2<K, Long>, K> {

		@Override
		public K map(Tuple2<K, Long> value) throws Exception {
			return value.f0;
		}
    	
    }
    
  //NOTE: https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/libs/gelly/graph_api.html
    
    public static <K, VV, EV> DataSet<K>  expandedDeltaVertexIds(final Graph<K, VV, EV> originalGraph, final DataSet<Tuple2<K, Double>> kHotVertices, 
    		final long level, final Map<Long, UpdateInfo> infos, final DataSet<Tuple2<K, Double>> previousRanks, final double delta) throws Exception {
    	    	
    	// Calculate personalized vertex scores.
    	DataSet<Tuple2<K, Long>> expandedIds = kHotVertices
    			.join(previousRanks)
    			.where(0).equalTo(0)
    			.map(new CustomNeighborhoodMapFunction<K>(level, delta, infos));
    	
    	// Find out the maximum value.    	
    	Long maxN = expandedIds.maxBy(1).collect().get(0).f1;
    	final Long printN = maxN;
    	while (maxN > 0) {
            final DataSet<Tuple2<K, Long>> firstNeighbours = expandedIds
            		.filter(new PositiveNeighborhoodFilterFunction<K>())
                    .join(originalGraph.getEdges())
                    .where(0).equalTo(0)
                    .with((id, e) -> Tuple2.of(e.getTarget(), id.f1 - 1))
                    .returns(expandedIds.getType())
                    .name("Neighbours level " + level);

            expandedIds = expandedIds
            		.map(new NeighborhoodHopMapFunction<K>())
            		.union(firstNeighbours).distinct().name("Expanded level " + level);
            maxN--;
        }

    	System.out.println("maxN=" + printN);
    	final DataSet<K> individualVertices = expandedIds.map(new RetainVertexIdMapFunction<K>());
    	
    	return individualVertices;
	}
    
    public static <K, VV, EV> DataSet<K>  expandedDeltaFixed(final Graph<K, VV, EV> originalGraph, final DataSet<Tuple2<K, Double>> kHotVertices, 
    		final long level, final Map<Long, UpdateInfo> infos, final DataSet<Tuple2<K, Double>> previousRanks, final double delta) throws Exception {
    	    	
    	
    	
    	// Calculate personalized vertex scores.
    	DataSet<Tuple2<K, Long>> expandedIds = kHotVertices
    			.join(previousRanks)
    			.where(0).equalTo(0)
    			.map(new CustomNeighborhoodMapFunction<K>(level, delta, infos));
    	
    	// Find out the maximum value.    	
    	Long maxN = expandedIds.maxBy(1).collect().get(0).f1;
    	final Long printN = maxN;
    	while (maxN > 0) {
            final DataSet<Tuple2<K, Long>> firstNeighbours = expandedIds
            		.filter(new PositiveNeighborhoodFilterFunction<K>())
                    .join(originalGraph.getEdges())
                    .where(0).equalTo(0)
                    .with((id, e) -> Tuple2.of(e.getTarget(), id.f1 - 1))
                    .returns(expandedIds.getType())
                    .name("Neighbours level " + level);

            expandedIds = expandedIds
            		.map(new NeighborhoodHopMapFunction<K>())
            		.union(firstNeighbours).distinct().name("Expanded level " + level);
            maxN--;
        }

    	System.out.println("maxN=" + printN);
    	final DataSet<K> individualVertices = expandedIds.map(new RetainVertexIdMapFunction<K>());
    	
    	return individualVertices;
	}
    

    public static <K, VV, EV> DataSet<Edge<K, EV>> selectEdges(final Graph<K, VV, EV> originalGraph, final DataSet<Vertex<K, VV>> vertices) {
        return vertices
                .joinWithHuge(originalGraph.getEdges())
                .where(0).equalTo(0)
                .with((source, edge) -> edge)
                .returns(originalGraph.getEdges().getType())
                .join(vertices)
                .where(1).equalTo(0)
                .with((e, v) -> e)
                .returns(originalGraph.getEdges().getType())
                .distinct(0, 1);
    }

    public static <K, VV, EV> DataSet<Edge<K, EV>> externalEdges(final Graph<K, VV, EV> originalGraph, final DataSet<Edge<K, EV>> edgesToBeRemoved) {
        return originalGraph
				.getEdges()
				.coGroup(edgesToBeRemoved)
                .where(0, 1).equalTo(0, 1)
                .with((Iterable<Edge<K, EV>> edge, Iterable<Edge<K, EV>> edgeToBeRemoved, Collector<Edge<K, EV>> out) -> {
                    if (!edgeToBeRemoved.iterator().hasNext()) {
                        for (Edge<K, EV> next : edge) {
                            out.collect(next);
                        }
                    }
                }).returns(originalGraph.getEdges().getType());
    }

    private static class VertexKeySelector<K> implements KeySelector<K, K>, ResultTypeQueryable<K> {
		final TypeInformation<K> type;

        public VertexKeySelector(final TypeInformation<K> type) {
            this.type = type;
        }

        @Override
        public K getKey(final K value) throws Exception {
            return value;
        }

        @Override
        public TypeInformation<K> getProducedType() {
            return type;
        }
    }

    public static class EdgeToTuple2<K, EV> implements MapFunction<Edge<K, EV>, Tuple2<K, K>> {


		@Override
        public Tuple2<K, K> map(final Edge<K, EV> edge) throws Exception {
            return Tuple2.of(edge.getSource(), edge.getTarget());
        }
    }
    
    public static class FilterDanglingNodes<K> implements FilterFunction<Tuple2<K, LongValue>> {

		@Override
		public boolean filter(Tuple2<K, LongValue> arg0) throws Exception {
			return arg0.f1.getValue() > 1;
		}	
    }
    
    public static class JoinNonDanglingNodes<VV> implements VertexJoinFunction<VV, LongValue> {

		@Override
		public VV vertexJoin(VV vertexValue, LongValue arg1) throws Exception {
			return vertexValue;
		}
    	
    }
}
