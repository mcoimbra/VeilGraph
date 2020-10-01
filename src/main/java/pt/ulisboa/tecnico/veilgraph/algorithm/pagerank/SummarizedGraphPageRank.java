package pt.ulisboa.tecnico.veilgraph.algorithm.pagerank;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;

import java.io.Serializable;

/**
 * PageRank for a summary graph, with a big vertex that should be handled in a special way.
 *
 * @author Renato Rosa
 */
public class SummarizedGraphPageRank<K> implements GraphAlgorithm<K, Double, Double, DataSet<Tuple2<K, Double>>>, Serializable {

	private static final long serialVersionUID = -7478304839520940562L;
	private final Double beta;
    private final Integer iterations;
    private final Long bigVertexId;

    public SummarizedGraphPageRank(final Double beta, final Integer iterations, final Long bigVertexId) {
        this.beta = beta;
        this.bigVertexId = bigVertexId;
        this.iterations = iterations;
    }

    @Override
    public DataSet<Tuple2<K, Double>> run(final Graph<K, Double, Double> graph) {
    	final ScatterGatherConfiguration conf = new ScatterGatherConfiguration();
        conf.setName("Simple Approximated PageRank");
        conf.setDirection(EdgeDirection.OUT);
        return graph.runScatterGatherIteration(new RankMessenger(), new VertexRankUpdater(), iterations, conf).getVerticesAsTuple2();
    }

    private class VertexRankUpdater extends GatherFunction<K, Double, Double> {
		private static final long serialVersionUID = 6217286535185596855L;

		@Override
        public void updateVertex(final Vertex<K, Double> vertex, final MessageIterator<Double> inMessages) {
            if (vertex.getId().equals(bigVertexId)) {
                // do not change the rank of the big vertex
                setNewVertexValue(vertex.getValue()); //necessary to assure messages are sent next
                return;
            }

            double rankSum = 0.0;
            for (final double msg : inMessages) {
                rankSum += msg;
            }

            // apply the dampening factor / random jump
            final double newRank = (beta * rankSum) + (1 - beta);// / this.getNumberOfVertices();
            setNewVertexValue(newRank);
        }
    }

    private class RankMessenger extends ScatterFunction<K, Double, Double, Double> {
		private static final long serialVersionUID = 6671947675275223679L;
		@Override
        public void sendMessages(final Vertex<K, Double> vertex) throws Exception {
            if (vertex.getId().equals(bigVertexId)) {
                for (final Edge<K, Double> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), edge.getValue());
                }
            } else {
                for (final Edge<K, Double> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
                }
            }

            // dummy message to force computation for every vertex
            sendMessageTo(vertex.getId(), 0.0);
        }
    }
}

