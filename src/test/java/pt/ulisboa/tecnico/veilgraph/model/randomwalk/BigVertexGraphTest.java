package pt.ulisboa.tecnico.veilgraph.model.randomwalk;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;

import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.*;

import pt.ulisboa.tecnico.veilgraph.model.GraphModelFactory;
import pt.ulisboa.tecnico.veilgraph.pt.ulisboa.tecnico.graph.AbstractGraphTest;
import pt.ulisboa.tecnico.veilgraph.stream.GraphUpdateTracker;

import java.util.ArrayList;
import java.util.Map;

/**
 * A JUnit Jupiter class to test the big vertex graph model.
 *
 * @see http://junit.org/junit5/docs/current/user-guide/#writing-tests
 * @author Miguel E. Coimbra
 *
 */
@Tag("fast")
@DisplayName("Big vertex graph model tests")
@SuppressWarnings("unused")
public class BigVertexGraphTest extends AbstractGraphTest {


    private final ArrayList<DataSet<Tuple2<Long, Double>>> offlineResults = new ArrayList<DataSet<Tuple2<Long, Double>>>();




    /**
     * Private constructor for the singleton pattern.
     */
    private BigVertexGraphTest() {}

    /**
     * Singleton design pattern instance access.
     * @return the singleton instance.
     */
    protected static BigVertexGraphTest getInstance() {
        if(singleton == null)
            singleton = new BigVertexGraphTest();
        return (BigVertexGraphTest) singleton;
    }



    private static final class EdgeRemovalCoGroup<K, EV> implements CoGroupFunction<Edge<K, EV>, Edge<K, EV>, Edge<K, EV>> {

        private static final long serialVersionUID = -8702181599856983541L;

        @Override
        public void coGroup(Iterable<Edge<K, EV>> edge, Iterable<Edge<K, EV>> edgeToBeRemoved,
                            Collector<Edge<K, EV>> out) throws Exception {
            if (!edgeToBeRemoved.iterator().hasNext()) {
                for (Edge<K, EV> next : edge) {
                    out.collect(next);
                }
            }
        }
    }

    @BeforeAll
    protected static void initAll() {
        final BigVertexGraphTest instance = BigVertexGraphTest.getInstance();
        instance.setup();

    }



    @BeforeEach
    private void init(final TestInfo testInfo) {
        LOG.info(() -> String.format("[%s]", testInfo.getDisplayName()));
    }

    @AfterEach
    private void tearDown(final TestInfo testInfo) {
        LOG.info(() -> String.format("[%s]" + System.lineSeparator(), testInfo.getDisplayName()));
    }

    @Test
    @DisplayName("testSummaryGraphCreation")
    private void testSummaryGraphCreation(final TestInfo testInfo) {
        final BigVertexGraphTest instance = BigVertexGraphTest.getInstance();

        final Map<String, Object> argValues = null;

        // Configure the big vertex representation.
        final GraphModelFactory<NullValue, NullValue> gmf = new GraphModelFactory<>();
        final BigVertexGraph model = (BigVertexGraph) gmf.getGraphModel(GraphModelFactory.Model.BIG_VERTEX, argValues, "");

        final DataSet<Tuple2<Long, GraphUpdateTracker.UpdateInfo>> infoDataSet = null;

       // final DataSet<Tuple2<Long, Double>> previousRanks = instance.getOfflineResults().get(0);

        // Produce the summarized Flink graph instance.
//        final Graph<Long, Double, Double> summaryGraph = model.getGraph(this.env, this.graph, infoDataSet, previousRanks);
    }
}
