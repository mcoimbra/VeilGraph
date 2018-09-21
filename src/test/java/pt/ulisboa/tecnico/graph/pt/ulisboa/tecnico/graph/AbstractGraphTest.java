package pt.ulisboa.tecnico.graph.pt.ulisboa.tecnico.graph;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import pt.ulisboa.tecnico.graph.GraphBoltTest;

import pt.ulisboa.tecnico.graph.stream.GraphUpdateTracker;


import java.io.File;
import java.util.ArrayList;
import java.util.logging.Logger;

@SuppressWarnings("unused")
public abstract class AbstractGraphTest {

    protected static final Logger LOG = Logger.getLogger(AbstractGraphTest.class.getName());
    protected static AbstractGraphTest singleton;
    protected GraphUpdateTracker<Long, NullValue, NullValue> tracker;
    protected ExecutionEnvironment env;


    protected ArrayList<Graph<Long, NullValue, NullValue>> graph = new ArrayList<>();
    protected ArrayList<Long> vertexCount = new ArrayList<Long>();
    protected ArrayList<Long> edgeCount = new ArrayList<Long>();
    protected final boolean debugging = true;

    /**
     * Getter for the global GraphBolt update tracking module instance.
     * This is used throughout the tests of this class to verify the behavior of graph update stream consumption.
     *
     * @see pt.ulisboa.tecnico.graph.stream.GraphUpdateTracker
     * @return the GraphUpdateTracker.
     */
    protected GraphUpdateTracker<Long, NullValue, NullValue> getUpdateTracker() {
        return this.tracker;
    }


    /**
     * Getter for the list of computed graphs.
     * There is an initial graph that is stored on index zero (0).
     * For each set of updates added to the graph, the new graph is computed and placed on the following index.
     * The first graph is on index zero (0) and after update zero (0), the second graph is on index one (1).
     * @return the list of computed graphs.
     */
    protected ArrayList<Graph<Long, NullValue, NullValue>> getGraphList() {
        return this.graph;
    }

    /**
     * Getter for the list of computed graph vertex counts.
     * @return the list of computed vertex counts.
     */
    protected ArrayList<Long> getVertexCountList() {
        return this.vertexCount;
    }

    /**
     * Getter for the list of computed graph edge counts.
     * @return the list of computed edge counts.
     */
    protected ArrayList<Long> getEdgeCountList() {
        return this.edgeCount;
    }

    /**
     * Setter for the global execution environment.
     * @param context the target Apache Flink execution environment.
     */
    private void setContext(final ExecutionEnvironment context) {
        this.env = context;
    }

    /**
     * Getter for the global execution environment.
     * @return the global execution environment.
     */
    protected ExecutionEnvironment getContext() {
        return this.env;
    }

    /**
     * Auxiliary function to know if output is to be verbose.
     * @return whether we are in debug mode or not.
     */
    private boolean debugging() {
        return this.debugging;
    }




    private void setUpdateTracker(GraphUpdateTracker<Long, NullValue, NullValue> tracker) {
        this.tracker = tracker;
    }

    protected void setup() {

        final ExecutionEnvironment context = ExecutionEnvironment.getExecutionEnvironment();

        singleton.setContext(context);

        context.getConfig().disableSysoutLogging();

        try {
            final String step0GraphPath = new File( GraphBoltTest.class.getResource("/step_0_graph.tsv").toURI()).getPath();
            final Graph<Long, NullValue, NullValue> g0 = Graph.fromCsvReader(step0GraphPath, context)
                    .ignoreCommentsEdges("#").fieldDelimiterEdges("\t").keyType(Long.class);
            final long vertexCount0 = g0.numberOfVertices();
            final long edgeCount0 = g0.numberOfEdges();
            singleton.getGraphList().add(g0);
            singleton.getVertexCountList().add(vertexCount0);
            singleton.getEdgeCountList().add(edgeCount0);



            final String step1GraphPath = new File( GraphBoltTest.class.getResource("/step_1_graph.tsv").toURI()).getPath();
            final Graph<Long, NullValue, NullValue> g1 = Graph.fromCsvReader(step1GraphPath, context)
                    .ignoreCommentsEdges("#").fieldDelimiterEdges("\t").keyType(Long.class);
            final long vertexCount1 = g1.numberOfVertices();
            final long edgeCount1 = g1.numberOfEdges();
            singleton.getGraphList().add(g1);
            singleton.getVertexCountList().add(vertexCount1);
            singleton.getEdgeCountList().add(edgeCount1);

            // Fill the update tracker with the edges that were added from g0 to g1.
            final GraphUpdateTracker<Long, NullValue, NullValue> tracker = new GraphUpdateTracker<>(g0);
            tracker.resetAll();

            final DataSet<Edge<Long, NullValue>> edgesToBeRemoved = g0.getEdges();

            /*
            final DataSet<Edge<Long, NullValue>> delta0 = g1.getEdges().coGroup(edgesToBeRemoved)
                    .where(0, 1).equalTo(0, 1).with(new BigVertexGraphTest.EdgeRemovalCoGroup<Long, NullValue>()).name("Remove edges");


            for(final Edge<Long, NullValue> e : delta0.collect()) {
                tracker.addEdge(e);
            }

            singleton.setUpdateTracker(tracker);
            */

        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
