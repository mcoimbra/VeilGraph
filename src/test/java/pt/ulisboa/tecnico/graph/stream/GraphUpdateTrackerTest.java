package pt.ulisboa.tecnico.graph.stream;


import org.junit.jupiter.api.*;
import pt.ulisboa.tecnico.graph.pt.ulisboa.tecnico.graph.AbstractGraphTest;


public class GraphUpdateTrackerTest extends AbstractGraphTest {
/*
    private static GraphUpdateTrackerTest singleton;
    private GraphUpdateTracker<Long, NullValue, NullValue> tracker;
    private ArrayList<Graph<Long, NullValue, NullValue>> g = new ArrayList<>();
    private ArrayList<Long> vertexCount = new ArrayList<Long>();
    private ArrayList<Long> edgeCount = new ArrayList<Long>();
    private final boolean debugging = true;

    */


    /**
     * Private constructor for the singleton pattern.
     */
    private GraphUpdateTrackerTest() {}

    /**
     * Singleton design pattern instance access.
     * @return the singleton instance.
     */
    private static GraphUpdateTrackerTest getInstance() {
        if(singleton == null)
            singleton = new GraphUpdateTrackerTest();
        return (GraphUpdateTrackerTest) singleton;
    }



    @Test
    @DisplayName("testDelta0EdgeCount")
    void testDelta0EdgeCount(final TestInfo testInfo) {
        try {
            final GraphUpdateTrackerTest instance = getInstance();

            int trackerAddedEdges = instance.getUpdateTracker().getGraphUpdates().edgesToAdd.size();

            Assertions.assertEquals(6, trackerAddedEdges, () -> "From step 0 to step 1 there should be 6 added edges.");
        } catch (final Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("testStep0VertexCount")
    void testStep0VertexCount(final TestInfo testInfo) {
        try {
            final GraphUpdateTrackerTest instance = getInstance();
            final long numberOfVertices = instance.getGraphList().get(0).numberOfVertices();
            Assertions.assertEquals(instance.getVertexCountList().get(0).longValue(), numberOfVertices, () -> "Vertex count mismatch.");
        } catch (final Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("testStep1VertexCount")
    void testStep1VertexCount(final TestInfo testInfo) {
        try {
            final GraphUpdateTrackerTest instance = getInstance();
            final long numberOfVertices = instance.getGraphList().get(1).numberOfVertices();
            Assertions.assertEquals(instance.getVertexCountList().get(1).longValue(), numberOfVertices, () -> "Vertex count mismatch.");
        } catch (final Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("testStep0EdgeCount")
    void testStep0EdgeCount(final TestInfo testInfo) {
        try {
            final GraphUpdateTrackerTest instance = getInstance();
            final long numberOfEdges = instance.getGraphList().get(0).numberOfEdges();
            Assertions.assertEquals(instance.getEdgeCountList().get(0).longValue(), numberOfEdges, () -> "Edge count mismatch.");
        } catch (final Exception e) {
            Assertions.fail(e.getMessage());
        }
    }
}
