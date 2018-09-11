package pt.ulisboa.tecnico.graph.model.randomwalk;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;

import java.io.Serializable;
import java.util.function.Function;

public class VertexCentricAlgorithm implements GraphAlgorithm<Long, Double, Double, DataSet<Tuple2<Long, Double>>>, Serializable {


    private static final long serialVersionUID = 1015118158273884539L;
    private final Integer iterations;
    private final Function<Double, Double> userFunction;
    private final Long bigVertexId;

    public VertexCentricAlgorithm(final Integer iterations, final Function<Double, Double> userFunction) {
        this.iterations = iterations;
        this.userFunction = userFunction;
        this.bigVertexId = null;
    }

    @Override
    public DataSet<Tuple2<Long, Double>> run(Graph<Long, Double, Double> graph) {
        final ScatterGatherConfiguration conf = new ScatterGatherConfiguration();
        conf.setName("Simple Approximated PageRank");
        conf.setDirection(EdgeDirection.OUT);

        // Run the scatter-gather algorithm.
        final DataSet<Tuple2<Long, Double>> result = graph.runScatterGatherIteration(
                new RankMessenger(),
                new VertexRankUpdater(),
                this.iterations,
                conf)
                .getVerticesAsTuple2();

        // Remove the big vertex from the results.
        final DataSet<Tuple2<Long, Double>> resultWithoutBigVertex = result
                .filter(t -> (t.f0 != this.bigVertexId));

        return resultWithoutBigVertex;
    }




    private class VertexRankUpdater extends GatherFunction<Long, Double, Double> {
        private static final long serialVersionUID = 5135398227833194437L;


        /*
        private transient final Function<Double, Double> userFunction;


        public VertexRankUpdater(Function<Double, Double> userFunction) {
            this.userFunction = userFunction;
        }

        */

        @Override
        public void updateVertex(final Vertex<Long, Double> vertex, final MessageIterator<Double> inMessages) {
            if (vertex.getId().equals(bigVertexId)) {
                // do not change the rank of the big vertex
                setNewVertexValue(vertex.getValue()); //necessary to assure messages are sent next
                return;
            }

            double rankSum = 0.0;
            for (double msg : inMessages) {
                rankSum += msg;
            }

            // apply the dampening factor / random jump
            double newRank = userFunction.apply(rankSum);

            setNewVertexValue(newRank);
        }
    }

    private class RankMessenger extends ScatterFunction<Long, Double, Double, Double> {
        private static final long serialVersionUID = -6689324748824976903L;
        @Override
        public void sendMessages(Vertex<Long, Double> vertex) throws Exception {
            if (vertex.getId().equals(bigVertexId)) {
                for (final Edge<Long, Double> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), edge.getValue());
                }
            } else {
                for (final Edge<Long, Double> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
                }
            }

            // dummy message to force computation for every vertex
            sendMessageTo(vertex.getId(), 0.0);
        }
    }
}
