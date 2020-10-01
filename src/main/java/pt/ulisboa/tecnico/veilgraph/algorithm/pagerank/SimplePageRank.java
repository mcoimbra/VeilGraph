package pt.ulisboa.tecnico.veilgraph.algorithm.pagerank;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.LongValue;

import java.io.Serializable;

/**
 * Flink's PageRank adapted to support vertices without incoming or outgoing edges, and from any type.
 *
 * @param <K>
 * @param <VV>
 * @param <EV>
 * @author Renato Rosa
 */
public class SimplePageRank<K, VV, EV> implements GraphAlgorithm<K, VV, EV, DataSet<Tuple2<K, Double>>>, Serializable {

	private static final long serialVersionUID = -6058089866338381997L;
	private final Double beta;
    private final Integer iterations;
    private final Double initialRank;
    private transient final DataSet<Tuple2<K, Double>> initialRanks;

    //private int executionCounter = -1;

    public SimplePageRank(final double beta, final double initialRank, final int iterations) {
        this.beta = beta;
        this.initialRank = initialRank;
        this.initialRanks = null;
        this.iterations = iterations;
    }

    public SimplePageRank(final double beta, final DataSet<Tuple2<K, Double>> initialRanks, final int iterations) {
        this.beta = beta;
        this.initialRank = 1.0 ;
        this.initialRanks = initialRanks;
        this.iterations = iterations;
    }

    @Override
    public DataSet<Tuple2<K, Double>> run(final Graph<K, VV, EV> graph) {
    	final DataSet<Tuple2<K, LongValue>> vertexOutDegrees = graph.outDegrees();

        Graph<K, Double, Double> g = graph
                .mapVertices(new RankInitializer())
                .mapEdges(new EdgeInitializer())
                .joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

        if (this.initialRanks != null) {
            g = g.joinWithVertices(this.initialRanks, new RanksJoinFunction());
        }

        final ScatterGatherConfiguration conf = new ScatterGatherConfiguration();

       // if(this.executionCounter == -1)
        conf.setName("Simple PageRank");
       // else
        //    conf.setName("Simple PageRank " + this.executionCounter);
        conf.setDirection(EdgeDirection.OUT);

        return g.runScatterGatherIteration(new RankMessenger(), new VertexRankUpdater(), this.iterations, conf)
                .getVerticesAsTuple2();
    }

    @FunctionAnnotation.ForwardedFieldsFirst("*->*")
    private static class RanksJoinFunction implements VertexJoinFunction<Double, Double> {
        /**
		 * 
		 */
		private static final long serialVersionUID = -4043222309973192556L;

		@Override
        public Double vertexJoin(final Double vertexValue, final Double inputValue) throws Exception {
            return inputValue;
        }
    }

    private static class InitWeights implements EdgeJoinFunction<Double, LongValue> {
        /**
		 * 
		 */
		private static final long serialVersionUID = -2404621604932466157L;

		@Override
        public Double edgeJoin(final Double edgeValue, final LongValue inputValue) {
            return edgeValue / (double) inputValue.getValue();
        }
    }

    private class VertexRankUpdater extends GatherFunction<K, Double, Double> {
		private static final long serialVersionUID = 5135398227833194437L;

		@Override
        public void updateVertex(final Vertex<K, Double> vertex, final MessageIterator<Double> inMessages) {
            double rankSum = 0.0;
            for (double msg : inMessages) {
                rankSum += msg;
            }

            // apply the dampening factor / random jump
            double newRank = (beta * rankSum) + 1 - beta;

            //double newRank = (beta * rankSum) + 1 - beta / this.getNumberOfVertices();
            setNewVertexValue(newRank);
        }
    }

    private class RankMessenger extends ScatterFunction<K, Double, Double, Double> {
    	private static final long serialVersionUID = -6689324748824976903L;
        @Override
        public void sendMessages(Vertex<K, Double> vertex) throws Exception {
            for (Edge<K, Double> edge : getEdges()) {
                sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
            }
            // dummy message to force computation for every vertex
            sendMessageTo(vertex.getId(), 0.0);
        }
    }

    private class RankInitializer implements MapFunction<Vertex<K, VV>, Double> {
		private static final long serialVersionUID = 4846506078331858312L;

		@Override
        public Double map(final Vertex<K, VV> v) throws Exception {
            //return initialRank;
			// changed 2017-07-27 based on https://github.com/apache/flink/blob/master/flink-libraries/flink-gelly-examples/src/main/java/org/apache/flink/graph/examples/PageRank.java
			return initialRank ;/// this.getNumberOfVertices(); //comment 2018-03-29

        }
    }

    private class EdgeInitializer implements MapFunction<Edge<K, EV>, Double> {
		private static final long serialVersionUID = 8698379106821623880L;

		@Override
        public Double map(final Edge<K, EV> value) throws Exception {
            return 1.0;
        }
    }
}

