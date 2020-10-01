package pt.ulisboa.tecnico.veilgraph;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.graph.*;
import org.apache.flink.graph.examples.data.PageRankData;
import org.apache.flink.types.LongValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import pt.ulisboa.tecnico.veilgraph.stream.GraphUpdateTracker;
import pt.ulisboa.tecnico.veilgraph.stream.GraphUpdateTracker.UpdateInfo;
//import pt.ulisboa.tecnico.graph.util.GraphUtils.GraphDoubleInitializer;
//import pt.ulisboa.tecnico.graph.util.GraphUtils.PageRankResultToDataSetMapper;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;


//import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.apache.flink.graph.library.linkanalysis.PageRank.Result;


import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;




@SuppressWarnings("unused")
public class GraphBoltTest
{
	private static final Logger LOG = Logger.getLogger(GraphBoltTest.class.getName());
	private static GraphBoltTest singleton = null;
	private ExecutionEnvironment context;
	private ArrayList<Graph<Long, NullValue, NullValue>> g = new ArrayList<>();
	private ArrayList<Long> vertexCount = new ArrayList<Long>();
	private ArrayList<Long> edgeCount = new ArrayList<Long>();
	private final boolean debugging = true;
	
	private final ArrayList<DataSet<Tuple2<Long, Double>>> offlineResults = new ArrayList<DataSet<Tuple2<Long, Double>>>();
	private GraphUpdateTracker<Long, NullValue, NullValue> tracker = null;
	
	private void setUpdateTracker(GraphUpdateTracker<Long, NullValue, NullValue> tracker) {
		this.tracker = tracker;
	}
	
	/**
	 * Getter for the global GraphBolt update tracking module instance.
	 * This is used throughout the tests of this class to verify the behavior of graph update stream consumption.
	 * 
	 * @see pt.ulisboa.tecnico.veilgraph.stream.GraphUpdateTracker
	 * @return the GraphUpdateTracker.
	 */
	private GraphUpdateTracker<Long, NullValue, NullValue> getUpdateTracker() {
		return this.tracker;
	}
	
	/**
	 * Private constructor for the singleton pattern.
	 */
	private GraphBoltTest() {}
	
	/**
	 * Singleton design pattern instance access.
	 * @return the singleton instance.
	 */
	private static GraphBoltTest getInstance() {
		if(singleton == null)
			singleton = new GraphBoltTest();
		return singleton;
	}
	
	/**
	 * Getter for the list of computed graphs.
	 * There is an initial graph that is stored on index zero (0).
	 * For each set of updates added to the graph, the new graph is computed and placed on the following index.
	 * The first graph is on index zero (0) and after update zero (0), the second graph is on index one (1).
	 * @return the list of computed graphs.
	 */
	private ArrayList<Graph<Long, NullValue, NullValue>> getGraphList() {
		return this.g;
	}
	
	/**
	 * Getter for the list of computed graph vertex counts.
	 * @return the list of computed vertex counts.
	 */
	private ArrayList<Long> getVertexCountList() {
		return this.vertexCount;
	}
	
	/**
	 * Getter for the list of computed graph edge counts.
	 * @return the list of computed edge counts.
	 */
	private ArrayList<Long> getEdgeCountList() {
		return this.edgeCount;
	}
	
	/**
	 * Setter for the global execution environment.
	 * @param context the target Apache Flink execution environment.
	 */
	private void setContext(final ExecutionEnvironment context) {
		this.context = context;
	}
	
	/**
	 * Getter for the global execution environment.
	 * @return the global execution environment.
	 */
	private ExecutionEnvironment getContext() {
		return this.context;
	}
	
	/**
	 * Auxiliary function to know if output is to be verbose.
	 * @return whether we are in debug mode or not.
	 */
	private boolean debugging() {
		return this.debugging;
	}
	
	
	/**
	 * Get the results for each computed PageRank step so far.
	 * @return the list of results.
	 */
	public ArrayList<DataSet<Tuple2<Long, Double>>> getOfflineResults() {
		return this.offlineResults;
	}
	

	
	
	
	@BeforeAll
	private static void initAll() {
		final GraphBoltTest instance = getInstance();
		final ExecutionEnvironment context = ExecutionEnvironment.getExecutionEnvironment();
		instance.setContext(context);
		
		context.getConfig().disableSysoutLogging();
		
		try {	
			final String step0GraphPath = new File( GraphBoltTest.class.getResource("/step_0_graph.tsv").toURI()).getPath();
			final Graph<Long, NullValue, NullValue> g0 = Graph.fromCsvReader(step0GraphPath, context)
					.ignoreCommentsEdges("#").fieldDelimiterEdges("\t").keyType(Long.class);		
			final long vertexCount0 = g0.numberOfVertices();
			final long edgeCount0 = g0.numberOfEdges();
			instance.getGraphList().add(g0);
			instance.getVertexCountList().add(vertexCount0);
			instance.getEdgeCountList().add(edgeCount0);
			
			final String step0RankPath = new File( GraphBoltTest.class.getResource("/step_0_python_powermethod_pr.tsv").toURI()).getPath();
			final DataSet<Tuple2<Long, Double>> ranks0 = getInstance().getContext()
					.readCsvFile(step0RankPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.types(Long.class, Double.class)
					.sortPartition(1, Order.DESCENDING);
			instance.getOfflineResults().add(ranks0);
			
			final String step1GraphPath = new File( GraphBoltTest.class.getResource("/step_1_graph.tsv").toURI()).getPath();
			final Graph<Long, NullValue, NullValue> g1 = Graph.fromCsvReader(step1GraphPath, context)
					.ignoreCommentsEdges("#").fieldDelimiterEdges("\t").keyType(Long.class);		
			final long vertexCount1 = g1.numberOfVertices();
			final long edgeCount1 = g1.numberOfEdges();
			instance.getGraphList().add(g1);
			instance.getVertexCountList().add(vertexCount1);
			instance.getEdgeCountList().add(edgeCount1);
			
			final String step1RankPath = new File( GraphBoltTest.class.getResource("/step_1_python_powermethod_pr.tsv").toURI()).getPath();
			final DataSet<Tuple2<Long, Double>> ranks1 = getInstance().getContext()
					.readCsvFile(step1RankPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.types(Long.class, Double.class)
					.sortPartition(1, Order.DESCENDING);
			
			instance.getOfflineResults().add(ranks1);
			
			// Fill the update tracker with the edges that were added from g0 to g1.
			final GraphUpdateTracker<Long, NullValue, NullValue> tracker = new GraphUpdateTracker<>(g0);
			tracker.resetAll();
			
			final DataSet<Edge<Long, NullValue>> edgesToBeRemoved = g0.getEdges();
			final DataSet<Edge<Long, NullValue>> delta0 = g1.getEdges().coGroup(edgesToBeRemoved)
					.where(0, 1).equalTo(0, 1).with(new EdgeRemovalCoGroup<Long, NullValue>()).name("Remove edges");
			
			
			for(final Edge<Long, NullValue> e : delta0.collect()) {
				tracker.addEdge(e);
			}

			instance.setUpdateTracker(tracker);
			
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	

	@BeforeEach
	private void init(final TestInfo testInfo) {
		LOG.info(() -> String.format("[%s]", testInfo.getDisplayName()));
	}
	
	@AfterEach
	private void tearDown(final TestInfo testInfo) {
		LOG.info(() -> String.format("[%s]" + System.lineSeparator(), testInfo.getDisplayName()));
	}
	
	/**
	 * Ensure that the number of vertices, edges and ranks increases with step number.
	 * @param testInfo
	 */
	@Test
	@DisplayName("testGraphStepProgression")
	void testGraphStepProgression(final TestInfo testInfo) {
		try {
			final GraphBoltTest instance = getInstance();
			

			for(int i = 1, lim = instance.getGraphList().size(); i < lim; i++) {
				
				final int printIndex = i;
				
				Assertions.assertTrue(
						instance.getGraphList().get(i).numberOfEdges() > instance.getGraphList().get(i-1).numberOfEdges(), 
						() -> String.format("Number of vertices should increase from step %d to %d.", printIndex-1, printIndex));
				
				Assertions.assertTrue(
						instance.getGraphList().get(i).numberOfVertices() > instance.getGraphList().get(i-1).numberOfVertices(), 
						() -> String.format("Number of edges should increase from step %d to %d.", printIndex-1, printIndex));
				
				Assertions.assertTrue(
						instance.getOfflineResults().get(i).count() > instance.getOfflineResults().get(i-1).count(), 
						() -> String.format("Size of graph ranks should increase from step %d to %d.", printIndex-1, printIndex));
			}
			

		} catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
	}
	

	
	/**
	 * Test the GraphBolt hot vertex set calculation with n=0 after the first set of updates is applied.
	 * @param testInfo
	 */

	@Test
	@DisplayName("testStep1ParameterExpansionN0")
	void testStep1ParameterExpansionN0(final TestInfo testInfo) {
		try {

			/*

			final GraphBoltTest instance = getInstance();
			final Double vertexDegreeMininumChangeRatio = new Double(0.05);
			final Map<Long, UpdateInfo> infos = instance.getUpdateTracker().getUpdateInfos();
			final DataSet<Tuple2<Long, Double>> previousRanks = instance.getOfflineResults().get(0);

			// Get vertices where the previous in-degree was 0 or it changed enough so that abs((curr-degree / prev-degree) - 1) > vertexDegreeMininumChangeRatio
			final List<Tuple2<Long, Double>> changedVertices = BigVertexGraph.getKHotVertices(infos, vertexDegreeMininumChangeRatio, EdgeDirection.IN);

			if(getInstance().debugging()) {
				// Ranks from the previous PageRank Python power-method execution.
				System.out.println("Previous ranks: ");
				previousRanks.print();

				// Print the UpdateInfo items corresponding to the initial graph.
				System.out.println("Update infos: ");
				for(Long u : infos.keySet()) {
					System.out.println(u + "\t " + instance.getUpdateTracker().getUpdateInfos().get(u));
				}
				System.out.println("Changed vertices: ");
				for(Tuple2<Long, Double> t : changedVertices) {
					System.out.println(t);
				}
			}


			// Set of vertices whose degree change is equal or more than vertexDegreeMininumChangeRatio: 6, 7, 10
			// Included in those sets are the vertices which are new: 12, 13, 14
			
			Assertions.assertEquals(6, changedVertices.size(), () -> "Expected six (6) changed vertices from step 0 to step 1.");
			
			// Vertices which did not previously have indegrees.
			final DataSet<Tuple2<Long, Double>> kHotVertices = instance.getContext().fromCollection(changedVertices);
			
			final long n = 0;
			final Double delta = new Double(0.0400d);
*/
/*
			
			final DataSet<Tuple2<Long, Long>> expandedIds = kHotVertices
	    			.join(previousRanks)
	    			.where(0).equalTo(0)
	    			.map(new ExpandVerticesMapper(n, delta, infos, EdgeDirection.IN));
			
			final List<Tuple2<Long, Long>> expandedVertexList = expandedIds.collect();

			if(getInstance().debugging()) {
				System.out.println("expandedVertices: (id, neighborhood size)");
				for (Tuple2<Long, Long> t : expandedVertexList) {
					System.out.println(t);
				}
			}
			
			// (7, 1), (6, 1), (10, 0)
			
			Assertions.assertEquals(3, expandedVertexList.size(), () -> "The hot vertex set is supposed to finish with three (3) vertices.");

			Integer iterationLimit = expandedIds.max(1).collect().get(0).f1.intValue();

			DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, getInstance().getGraphList().get(1), iterationLimit);
			//DataSet<Long> results = deltaExpansion(expandedIds, getInstance().getGraphList().get(1), (int)n);
			
			List<Long> resultsList = results.collect();
			
			for(Long t : resultsList) {
				System.out.println(t);
			}
			// (0, 6, 7, 10, 12)
			
			Assertions.assertEquals(5, resultsList.size(), () -> "The expanded vertex set is supposed to have five (5) vertices.");
			*/
			
		} catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
	}


	@Test
	@DisplayName("testDeltaIteration1")
	void testDeltaIteration1(final TestInfo testInfo) {

		final ExecutionEnvironment env = GraphBoltTest.getInstance().getContext();

		// Define some edges.
		final ArrayList<Tuple2<Long, Long>> edges = new ArrayList<Tuple2<Long, Long>>();
		edges.add(Tuple2.of(0L, 1L));
		edges.add(Tuple2.of(1L, 2L));
		edges.add(Tuple2.of(2L, 3L));
		edges.add(Tuple2.of(3L, 4L));
		final DataSet<Tuple2<Long, Long>> edgeDataSet = env.fromCollection(edges);

		// Build a small dummy graph.
		final Graph<Long, NullValue, NullValue> g = Graph.fromTuple2DataSet(edgeDataSet, env);

		// Define the starting expansion vertex for the DeltaIteration implementation.
		final ArrayList<Tuple2<Long, Long>> l = new ArrayList<Tuple2<Long, Long>>();

		final int iterationLimit = 3;
		l.add(Tuple2.of(0L, (long) iterationLimit));
		final DataSet<Tuple2<Long, Long>> expandedIds = env.fromCollection(l);
/*
		final DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, g, iterationLimit);

		try {
			final List<Long> resultsList = results.collect();
			if(getInstance().debugging()) {
				for (Long t : resultsList) {
					System.out.println(t);
				}
			}

			Assertions.assertEquals(4, resultsList.size(), () -> "The expanded vertex set is supposed to have four (4) vertices.");
		}  catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
		*/
	}

	@Test
	@DisplayName("testDeltaIteration2")
	void testDeltaIteration2(final TestInfo testInfo) {

		final ExecutionEnvironment env = GraphBoltTest.getInstance().getContext();

		// Define some edges.
		final ArrayList<Tuple2<Long, Long>> edges = new ArrayList<Tuple2<Long, Long>>();
		edges.add(Tuple2.of(0L, 1L));
		edges.add(Tuple2.of(1L, 2L));
		edges.add(Tuple2.of(2L, 3L));
		edges.add(Tuple2.of(3L, 4L));
		edges.add(Tuple2.of(1111L, 1L));
		final DataSet<Tuple2<Long, Long>> edgeDataSet = env.fromCollection(edges);

		// Build a small dummy graph.
		final Graph<Long, NullValue, NullValue> g = Graph.fromTuple2DataSet(edgeDataSet, env);

		// Define the starting expansion vertex for the DeltaIteration implementation.
		final ArrayList<Tuple2<Long, Long>> l = new ArrayList<Tuple2<Long, Long>>();

		final int iterationLimit = 1;
		l.add(Tuple2.of(0L, (long) iterationLimit));
		final DataSet<Tuple2<Long, Long>> expandedIds = env.fromCollection(l);
/*
		final DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, g, iterationLimit);

		try {
			final List<Long> resultsList = results.collect();
			if(getInstance().debugging()) {
				for (Long t : resultsList) {
					System.out.println(t);
				}
			}

			Assertions.assertEquals(2, resultsList.size(), () -> "The expanded vertex set is supposed to have two (2) vertices.");
		}  catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
		*/
	}


	@Test
	@DisplayName("testDeltaIteration3")
	void testDeltaIteration3(final TestInfo testInfo) {

		final ExecutionEnvironment env = GraphBoltTest.getInstance().getContext();

		// Define some edges.
		final ArrayList<Tuple2<Long, Long>> edges = new ArrayList<Tuple2<Long, Long>>();
		edges.add(Tuple2.of(0L, 1L));
		edges.add(Tuple2.of(1L, 2L));
		edges.add(Tuple2.of(2L, 3L));
		edges.add(Tuple2.of(3L, 4L));
		edges.add(Tuple2.of(1111L, 1L));
		final DataSet<Tuple2<Long, Long>> edgeDataSet = env.fromCollection(edges);

		// Build a small dummy graph.
		final Graph<Long, NullValue, NullValue> g = Graph.fromTuple2DataSet(edgeDataSet, env);

		// Define the starting expansion vertex for the DeltaIteration implementation.
		final ArrayList<Tuple2<Long, Long>> l = new ArrayList<Tuple2<Long, Long>>();

		final int iterationLimit = 2;
		l.add(Tuple2.of(0L, (long) 1));
		l.add(Tuple2.of(1111L, (long) iterationLimit));
		final DataSet<Tuple2<Long, Long>> expandedIds = env.fromCollection(l);
/*
		final DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, g, iterationLimit);

		try {
			final List<Long> resultsList = results.collect();
			if(getInstance().debugging()) {
				for (Long t : resultsList) {
					System.out.println(t);
				}
			}

			Assertions.assertEquals(4, resultsList.size(), () -> "The expanded vertex set is supposed to have four (4) vertices.");
		}  catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}

		*/
	}

	@Test
	@DisplayName("testDeltaIteration4")
	void testDeltaIteration4(final TestInfo testInfo) {

		final ExecutionEnvironment env = GraphBoltTest.getInstance().getContext();

		// Define some edges.
		final ArrayList<Tuple2<Long, Long>> edges = new ArrayList<Tuple2<Long, Long>>();
		edges.add(Tuple2.of(0L, 1L));
		edges.add(Tuple2.of(1L, 2L));
		edges.add(Tuple2.of(2L, 3L));
		edges.add(Tuple2.of(3L, 4L));
		edges.add(Tuple2.of(1111L, 1L));
		final DataSet<Tuple2<Long, Long>> edgeDataSet = env.fromCollection(edges);

		// Build a small dummy graph.
		final Graph<Long, NullValue, NullValue> g = Graph.fromTuple2DataSet(edgeDataSet, env);

		// Define the starting expansion vertex for the DeltaIteration implementation.
		final ArrayList<Tuple2<Long, Long>> l = new ArrayList<Tuple2<Long, Long>>();

		final int iterationLimit = 3;
		l.add(Tuple2.of(0L, (long) 1));
		l.add(Tuple2.of(1111L, (long) iterationLimit));
		final DataSet<Tuple2<Long, Long>> expandedIds = env.fromCollection(l);
/*
		final DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, g, iterationLimit);

		try {
			final List<Long> resultsList = results.collect();
			if(getInstance().debugging()) {
				for (Long t : resultsList) {
					System.out.println(t);
				}
			}

			Assertions.assertEquals(5, resultsList.size(), () -> "The expanded vertex set is supposed to have five (5) vertices.");
		}  catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
		*/
	}

	@Test
	@DisplayName("testDeltaIteration5")
	void testDeltaIteration5(final TestInfo testInfo) {

		final ExecutionEnvironment env = GraphBoltTest.getInstance().getContext();

		// Define some edges.
		final ArrayList<Tuple2<Long, Long>> edges = new ArrayList<Tuple2<Long, Long>>();
		edges.add(Tuple2.of(0L, 1L));
		edges.add(Tuple2.of(1L, 2L));
		edges.add(Tuple2.of(2L, 3L));
		edges.add(Tuple2.of(3L, 4L));
		edges.add(Tuple2.of(1111L, 1L));
		final DataSet<Tuple2<Long, Long>> edgeDataSet = env.fromCollection(edges);

		// Build a small dummy graph.
		final Graph<Long, NullValue, NullValue> g = Graph.fromTuple2DataSet(edgeDataSet, env);

		// Define the starting expansion vertex for the DeltaIteration implementation.
		final ArrayList<Tuple2<Long, Long>> l = new ArrayList<Tuple2<Long, Long>>();

		final int iterationLimit = 4;
		l.add(Tuple2.of(0L, (long) 1));
		l.add(Tuple2.of(1111L, (long) iterationLimit));
		final DataSet<Tuple2<Long, Long>> expandedIds = env.fromCollection(l);

		/*
		final DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, g, iterationLimit);

		try {
			final List<Long> resultsList = results.collect();
			if(getInstance().debugging()) {
				for (Long t : resultsList) {
					System.out.println(t);
				}
			}

			Assertions.assertEquals(6, resultsList.size(), () -> "The expanded vertex set is supposed to have six (6) vertices.");
		}  catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}

		*/
	}


	@Test
	@DisplayName("testDeltaIterationHundredIterations")
	void testDeltaIterationHundredIterations(final TestInfo testInfo) {

		final ExecutionEnvironment env = GraphBoltTest.getInstance().getContext();

		// Define some edges.
		final ArrayList<Tuple2<Long, Long>> edges = new ArrayList<Tuple2<Long, Long>>();
		edges.add(Tuple2.of(0L, 1L));
		edges.add(Tuple2.of(1L, 2L));
		edges.add(Tuple2.of(2L, 3L));
		edges.add(Tuple2.of(3L, 4L));
		edges.add(Tuple2.of(1111L, 1L));
		final DataSet<Tuple2<Long, Long>> edgeDataSet = env.fromCollection(edges);

		// Build a small dummy graph.
		final Graph<Long, NullValue, NullValue> g = Graph.fromTuple2DataSet(edgeDataSet, env);

		// Define the starting expansion vertex for the DeltaIteration implementation.
		final ArrayList<Tuple2<Long, Long>> l = new ArrayList<Tuple2<Long, Long>>();

		final int iterationLimit = 100;
		l.add(Tuple2.of(0L, (long) 1));
		l.add(Tuple2.of(1111L, (long) iterationLimit));
		final DataSet<Tuple2<Long, Long>> expandedIds = env.fromCollection(l);

		/*
		final DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, g, iterationLimit);

		try {
			final List<Long> resultsList = results.collect();
			if(getInstance().debugging()) {
				for (Long t : resultsList) {
					System.out.println(t);
				}
			}

			Assertions.assertEquals(6, resultsList.size(), () -> "The expanded vertex set is supposed to have six (6) vertices.");
		}  catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}

		*/
	}



	/**
	 * Test the GraphBolt hot vertex set calculation with n=1 after the first set of updates is applied.
	 * THIS TEST WAS NOT NOT INCLUDING NEWLY-ADDED VERTICES IN THE EXPANSION SET
	 * TODO: DELETE THIS LATER
	 * @param testInfo
	 */
	/*
	@Test
	@DisplayName("testStep1ParameterExpansionN1")
	void testStep1ParameterExpansionN1(final TestInfo testInfo) {
		try {
			final GraphBoltTest instance = getInstance();
			final Double vertexDegreeMininumChangeRatio = new Double(0.05);
			final Map<Long, UpdateInfo> infos = instance.getUpdateTracker().getUpdateInfos();
			final DataSet<Tuple2<Long, Double>> previousRanks = instance.getOfflineResults().get(0);

			// Get vertices where the previous in-degree was 0 or it changed enough so that abs((curr-degree / prev-degree) - 1) > vertexDegreeMininumChangeRatio
			final List<Tuple2<Long, Double>> changedVertices = BigVertexGraph.getKHotVertices(infos, vertexDegreeMininumChangeRatio, EdgeDirection.IN);

			if(getInstance().debugging()) {
				// Ranks from the previous PageRank Python power-method execution.
				System.out.println("Previous ranks: ");
				previousRanks.print();

				// Print the UpdateInfo items corresponding to the initial graph.
				System.out.println("Update infos: ");
				for(Long u : infos.keySet()) {
					System.out.println(u + "\t " + instance.getUpdateTracker().getUpdateInfos().get(u));
				}
				System.out.println("Changed vertices: ");
				for(Tuple2<Long, Double> t : changedVertices) {
					System.out.println(t);
				}
			}


			// Set of vertices whose degree change is equal or more than vertexDegreeMininumChangeRatio: 6, 7, 10
			// Included in those sets are the vertices which are new: 12, 13, 14

			Assertions.assertEquals(6, changedVertices.size(), () -> "Expected six (6) changed vertices from step 0 to step 1.");

			// Vertices which did not previously have indegrees.
			final DataSet<Tuple2<Long, Double>> kHotVertices = instance.getContext().fromCollection(changedVertices);

			final long n = 1;
			final Double delta = new Double(0.0400d);
*/
			/*
			NOTE: on Renato's version, new edges are added to the update tracker's infoMap.
			Later on in his computeApproximate, the update ids are fetched from infoMap, so they contain the newly-added vertices, just like this version.
			After that, Renato's BigVertexGraph keeps vertices which previously didn't exist and assigns them an initial value of 1.0d.
			Code below is what happens during summary graph building.

			.leftOuterJoin(previousRanks)
					.where(keySelector).equalTo(0)
					.with(new KernelVertexJoinFunction(initialRank))

			BOTTOMLINE: Renato's version vertex expansion is performed over the new vertices as well.
			DIFFERENCE: our expansion makes use of the rank, which is a problem for nodes new nodes, as they did not have a rank.
					*/


/*

			// Previous test without including new vertices in the expandedVertices (they had no previous rank so they would be left out on join(previousRanks)
			final DataSet<Tuple2<Long, Long>> expandedIds = kHotVertices
					.join(previousRanks)
					.where(0).equalTo(0)
					.map(new ExpandVerticesMapper(n, delta, infos, EdgeDirection.IN));


			final List<Tuple2<Long, Long>> expandedVertexList = expandedIds.collect();

			if(getInstance().debugging()) {
				System.out.println("expandedVertices: (id, neighborhood size)");
				for (Tuple2<Long, Long> t : expandedVertexList) {
					System.out.println(t);
				}
			}

			// (7, 2), (6, 2), (10, 1)

			Assertions.assertEquals(3, expandedVertexList.size(), () -> "The hot vertex set is supposed to finish with three (3) vertices.");

			Integer iterationLimit = expandedIds.max(1).collect().get(0).f1.intValue();

			System.out.println("Delta iteration limit: " + iterationLimit.toString());

			DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, getInstance().getGraphList().get(1), iterationLimit);
			//DataSet<Long> results = deltaExpansion(expandedIds, getInstance().getGraphList().get(1), (int)n);

			List<Long> resultsList = results.collect();

			for(Long t : resultsList) {
				System.out.println(t);
			}
			// (0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 12)

			Assertions.assertEquals(11, resultsList.size(), () -> "The expanded vertex set is supposed to have eleven (11) vertices.");

		} catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
	}
*/
	/**
	 * Test the GraphBolt hot vertex set calculation with n=1 after the first set of updates is applied.
	 * @param testInfo
	 */
	@Test
	@DisplayName("testStep1ParameterExpansionWithNewVerticesN1")
	void testStep1ParameterExpansionWithNewVerticesN1(final TestInfo testInfo) {
		try {
			final GraphBoltTest instance = getInstance();
			final Double r = new Double(0.05);
			final Map<Long, UpdateInfo> infos = instance.getUpdateTracker().getUpdateInfos();
			final DataSet<Tuple2<Long, Double>> previousRanks = instance.getOfflineResults().get(0);
/*
			// Get vertices where the previous in-degree was 0 or it changed enough so that abs((curr-degree / prev-degree) - 1) > vertexDegreeMininumChangeRatio
			final List<Tuple2<Long, Double>> changedVertices = BigVertexGraph.getKHotVertices(infos, r, EdgeDirection.IN);

			if(getInstance().debugging()) {
				// Ranks from the previous PageRank Python power-method execution.
				System.out.println("Previous ranks: ");
				previousRanks.print();

				// Print the UpdateInfo items corresponding to the initial graph.
				System.out.println("Update infos: ");
				for(Long u : infos.keySet()) {
					System.out.println(u + "\t " + instance.getUpdateTracker().getUpdateInfos().get(u));
				}
				System.out.println("Changed vertices: ");
				for(Tuple2<Long, Double> t : changedVertices) {
					System.out.println(t);
				}
			}


			// Set of vertices whose degree change is equal or more than vertexDegreeMininumChangeRatio: 6, 7, 10
			// Included in those sets are the vertices which are new: 12, 13, 14

			Assertions.assertEquals(6, changedVertices.size(), () -> "Expected six (6) changed vertices from step 0 to step 1.");

			// Vertices which did not previously have indegrees.
			final DataSet<Tuple2<Long, Double>> kHotVertices = instance.getContext().fromCollection(changedVertices);

			final long n = 1;
			final Double delta = new Double(0.0400d);
			*/
/*


			final DataSet<Tuple2<Long, Long>> expandedIds = kHotVertices
					.leftOuterJoin(previousRanks)
					.where(0).equalTo(0)
					.with(new JoinFunction<Tuple2<Long,Double>, Tuple2<Long,Double>, Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>>() {
						@Override
						public Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> join(Tuple2<Long, Double> longDoubleTuple2, Tuple2<Long, Double> longDoubleTuple22) throws Exception {
							return Tuple2.of(longDoubleTuple2, longDoubleTuple22);
						}
					})
					.map(new ExpandVerticesMapper(n, delta, infos, EdgeDirection.IN));




			final List<Tuple2<Long, Long>> expandedVertexList = expandedIds.collect();

			if(getInstance().debugging()) {
				System.out.println("expandedVertices: (id, neighborhood size)");
				for (Tuple2<Long, Long> t : expandedVertexList) {
					System.out.println(t);
				}
			}

			// (7, 2), (6, 2), (10, 1), (13, n), (14, n), (12, n)

			Assertions.assertEquals(6, expandedVertexList.size(), () -> "The hot vertex set is supposed to finish with six (6) vertices.");

			Integer iterationLimit = expandedIds.max(1).collect().get(0).f1.intValue();

			System.out.println("Delta iteration limit: " + iterationLimit.toString());

			DataSet<Long> results = null;
			//DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, getInstance().getGraphList().get(1), iterationLimit);

			List<Long> resultsList = results.collect();

			if(getInstance().debugging()) {
				for (Long t : resultsList) {
					System.out.println(t);
				}
			}
			// (0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 12, 13, 14)

			Assertions.assertEquals(13, resultsList.size(), () -> "The expanded vertex set is supposed to have thirteen (13) vertices.");
*/
		} catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
	}


	/**
	 * Test the GraphBolt hot vertex set calculation with n=1 after the first set of updates is applied.
	 * @param testInfo
	 */
	@Test
	@DisplayName("testStep1ParameterExpansionFullExpandCallN1")
	void testStep1ParameterExpansionFullExpandCallN1(final TestInfo testInfo) {
		final GraphBoltTest instance = getInstance();
		final ExecutionEnvironment env = instance.getContext();

		// Get vertices where the previous in-degree was 0 or it changed enough so that abs((curr-degree / prev-degree) - 1) > vertexDegreeMininumChangeRatio
		final Double r = new Double(0.05);
		final Integer n = 1;
		final Double delta = new Double(0.0400d);

		final Map<Long, UpdateInfo> infos = instance.getUpdateTracker().getUpdateInfos();
		//final DataSet<Tuple2<Long, UpdateInfo>> infoDataSet = ApproximatePageRank.infoMapToDataSet(infos, env);
		final DataSet<Tuple2<Long, Double>> previousRanks = instance.getOfflineResults().get(0);

		Graph<Long, NullValue, NullValue> g = getInstance().getGraphList().get(1);

		try {
			final DataSet<Long> targetVertices = null;
		//	final DataSet<Long> targetVertices = BigVertexGraph.expand(env, g, infoDataSet, previousRanks, r, n, delta, EdgeDirection.IN);


			if(getInstance().debugging()) {
				System.out.println("targetVertices: ");
				//targetVertices.print();
			}
			/* Vertices included after the delta iteration.
			resultsList
			 (0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 12, 13, 14)
			  */

			//Assertions.assertEquals(13, targetVertices.count(), () -> "The targetVertices DataSet is supposed to have thirteen (13) vertices.");


		} catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
	}
/*

	@Test
	@DisplayName("checkDeltaIterationInfiniteLoop2")
	void checkDeltaIterationInfiniteLoop2(final TestInfo testInfo) {
		final GraphBoltTest instance = getInstance();
		final ExecutionEnvironment env = instance.getContext();

		env.getConfig().disableSysoutLogging();

		Graph<Long, NullValue, NullValue> g;
		String graphPath = null;
		String expandedIdsPath = null;
		try {
			graphPath = new File(GraphBoltTest.class.getResource("/Facebook-links-5000-start.tsv").toURI()).getPath();
			expandedIdsPath = new File(GraphBoltTest.class.getResource("/expandedIds_1.csv").toURI()).getPath();
		}
		catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
		//g = Graph.fromCsvReader(graphPath, env).ignoreCommentsEdges("#").fieldDelimiterEdges("\t").keyType(Long.class);


		final ArrayList<Tuple2<Long, Long>> l = new ArrayList<Tuple2<Long, Long>>();

		//final int iterationLimit = 2;
		//l.add(Tuple2.of(59491L, (long) iterationLimit));
		//DataSet<Tuple2<Long, Long>> expandedIds = context.fromCollection(l);


		DataSet<Tuple2<Long, Long>> expandedIds = env.readCsvFile(expandedIdsPath).ignoreComments("#").fieldDelimiter("\t").types(Long.class, Long.class);//.keyType(Long.class);

		try {
			Integer maxDeltaIterations = expandedIds.max(1).collect().get(0).f1.intValue();
			System.out.println("Delta iteration limit: " + maxDeltaIterations.toString());


			//final DataSet<Long> expandedVertices = deltaExpansionSimplified(expandedIds, g, iterationLimit);
			// If there is no vertex upon which to expand, return the current vertex ids.

			final int keyPosition = 0;

			//TODO: test iterateDelta but set the initialDeltaSet to be equal to an initial expansion. THis means that we would do the same operations in the DeltaIteration body to initialize the initialDeltaSet, which would be fed to the DeltaIteration.

			// initialSolutionSet.iterateDelta(initialDeltaSet, maxDeltaIterations, keyPosition);
			// https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/index.html#iteration-operators
			final DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> deltaIteration =
					//expandedIds.iterateDelta(expandedIds, maxDeltaIterations, keyPosition);
					expandedIds.iterateDelta(expandedIds, maxDeltaIterations, keyPosition);

			// Retain only vertices that are to be expanded: their value is greater than zero (0).
			final DataSet<Tuple2<Long, Long>> positiveHops = deltaIteration
					.getWorkset()
					.filter(new KeepRanksAbove(0L))
					.name("DeltaIteration Workset Keep Above 0");

			// Join current vertices with out-edges.
			// This represents the out-neighbors of the vertices in positiveHops.
			final DataSet<Tuple2<Long, Long>> outVertices = positiveHops
					.leftOuterJoin(Graph.fromCsvReader(graphPath, env).ignoreCommentsEdges("#").fieldDelimiterEdges("\t").keyType(Long.class).getEdges()) // get out-edges
					.where(0)
					.equalTo(0)
					.with(new NeighborhoodHopper()) // move score to out-neighbors
					.groupBy(0)
					.reduce(new HopNormalizer()) // if an out-neighbor is reached by more than one vertex, keep the highest hops remaining
					.name("DeltaIteration positiveHops HopNormalizer");

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
						public boolean filter(Tuple2<Long, Long> longLongTuple2) throws Exception {
							return longLongTuple2.f1 == 0;
						}
					})
					.union(deltaIteration.getWorkset())
					.name("DeltaIteration nextWorkSet -> delta");

			final DataSet<Tuple2<Long, Long>> deltaIterationResult =
					deltaIteration.closeWith(delta, nextWorkset);

			DataSet<Long>  r = deltaIterationResult
					.map(new VertexIdExtractor())
					.name("DeltaIteration result VertexIdExtractor");

			System.out.println("Delta iteration result:");
			r.print();
		}
		catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}

		//final DataSet<Long> expandedVertices = deltaExpansion(expandedIds, graph, iterationLimit);
		//TESTING
		Assertions.assertEquals(true, true);


		//System.out.println(=)
	}

	@Test
	@DisplayName("checkDeltaIterationInfiniteLoop")
	void checkDeltaIterationInfiniteLoop(final TestInfo testInfo) {
		final GraphBoltTest instance = getInstance();
		final ExecutionEnvironment env = instance.getContext();

		env.getConfig().disableSysoutLogging();

		Graph<Long, NullValue, NullValue> g;
		String graphPath = null;
		String expandedIdsPath = null;
		try {
			graphPath = new File(GraphBoltTest.class.getResource("/Facebook-links-5000-start.tsv").toURI()).getPath();
			expandedIdsPath = new File(GraphBoltTest.class.getResource("/expandedIds_1.csv").toURI()).getPath();
		}
		catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
		g = Graph.fromCsvReader(graphPath, env)
				.ignoreCommentsEdges("#").fieldDelimiterEdges("\t").keyType(Long.class);


		final ArrayList<Tuple2<Long, Long>> l = new ArrayList<Tuple2<Long, Long>>();

		//final int iterationLimit = 2;
		//l.add(Tuple2.of(59491L, (long) iterationLimit));
		//DataSet<Tuple2<Long, Long>> expandedIds = context.fromCollection(l);


		DataSet<Tuple2<Long, Long>> expandedIds = env.readCsvFile(expandedIdsPath).ignoreComments("#").fieldDelimiter("\t").types(Long.class, Long.class);//.keyType(Long.class);

		try {
			Integer iterationLimit = expandedIds.max(1).collect().get(0).f1.intValue();
			System.out.println("Delta iteration limit: " + iterationLimit.toString());

			// ITERATION 1
			final DataSet<Tuple2<Long, Long>> positiveHops = expandedIds
					.filter(new KeepRanksAbove(0L))
					.name("DeltaIteration Workset Keep Above 0");

			System.out.println("positiveHops");
			positiveHops.print();

			// Join current vertices with out-edges.
			// This represents the out-neighbors of the vertices in positiveHops.
			final DataSet<Tuple2<Long, Long>> outVertices = positiveHops
					.leftOuterJoin(g.getEdges()) // get out-edges
					.where(0)
					.equalTo(0)
					.with(new NeighborhoodHopper()) // move score to out-neighbors
					.groupBy(0)
					.reduce(new HopNormalizer()) // if an out-neighbor is reached by more than one vertex, keep the highest hops remaining
					.name("DeltaIteration positiveHops HopNormalizer");

			System.out.println("outVertices");
			outVertices.print();

			// If a vertex in the current working set was reached in the expansion and its new expansion limit is bigger, store it.
			// repeatedExpansionVertices could be empty.
			final DataSet<Tuple2<Long, Long>> repeatedExpansionVertices = positiveHops
					.join(outVertices)
					.where(0)
					.equalTo(0)
					.with(new RepeatedExpansionNormalizer()) // repeated vertices with a lower score than original will have score set to -1
					.name("DeltaIteration positiveHops RepeatedExpansionNormalizer");
			System.out.println("repeatedExpansionVertices");
			repeatedExpansionVertices.print();

			// nextWorkset contains new vertices included in the expansion and old vertices whose propagated expansion hop value is greater than the original value in the current workset.
			final DataSet<Tuple2<Long, Long>> nextWorkset = outVertices // outVertices has no repeated keys
					.union(repeatedExpansionVertices) // repeatedExpansionVertices has no repeated keys
					.groupBy(0)
					.reduce(new RepeatedVertexReducer())  // if a group of 2 repeated vertices has a vertex with score -1L, we retain the -1L score
					.filter(new KeepRanksAbove(-1L)) //remove negative scores.
					.name("DeltaIteration outVertices RepeatedVertexReducer");
			System.out.println("nextWorkset");
			nextWorkset.print();


			final DataSet<Tuple2<Long, Long>> delta = nextWorkset
					.filter(new FilterFunction<Tuple2<Long, Long>>() { // include the newly-reached vertices with hop count = 0
						@Override
						public boolean filter(Tuple2<Long, Long> longLongTuple2) throws Exception {
							return longLongTuple2.f1 == 0;
						}
					})
					.union(expandedIds)
					.name("DeltaIteration nextWorkSet -> delta");
			System.out.println("delta");
			delta.print();


			// ITERATION 2 TODO TODO

			System.out.println("################");
			System.out.println("################");
			System.out.println("################");
			System.out.println("################");

			final DataSet<Tuple2<Long, Long>> positiveHops2 = nextWorkset
					.filter(new KeepRanksAbove(0L))
					.name("DeltaIteration Workset Keep Above 0");

			System.out.println("positiveHops");
			positiveHops2.print();

			// Join current vertices with out-edges.
			// This represents the out-neighbors of the vertices in positiveHops.
			System.out.println("|g.getEdges()| = " + g.getEdges().count());
			g = Graph.fromCsvReader(graphPath, env).ignoreCommentsEdges("#").fieldDelimiterEdges("\t").keyType(Long.class);
			final DataSet<Tuple2<Long, Long>> outVerticesIntermediate2 = positiveHops2
					//.leftOuterJoin(g.getEdges()) // get out-edges
					.joinWithHuge(g.getEdges())
					.where(0)
					.equalTo(0)
					.with(new FlatJoinFunction<Tuple2<Long, Long>, Edge<Long, NullValue>, Tuple2<Long, Long>>() {
						@Override
						public void join(Tuple2<Long, Long> longLongTuple2, Edge<Long, NullValue> longNullValueEdge, Collector<Tuple2<Long, Long>> collector) throws Exception {
							collector.collect(Tuple2.of(longNullValueEdge.f1, longLongTuple2.f1));
						}
					});
					//.with(new NeighborhoodHopper()); // move score to out-neighbors

			System.out.println("outVerticesIntermediate2");
			System.out.println("|outVerticesIntermediate2| = " + outVerticesIntermediate2.count());
			outVerticesIntermediate2.print();
			final DataSet<Tuple2<Long, Long>> outVertices2 = outVerticesIntermediate2
					.groupBy(0)
					.reduce(new HopNormalizer()) // if an out-neighbor is reached by more than one vertex, keep the highest hops remaining
					.name("DeltaIteration positiveHops HopNormalizer");

			System.out.println("outVertices2");
			outVertices2.print();

			// If a vertex in the current working set was reached in the expansion and its new expansion limit is bigger, store it.
			// repeatedExpansionVertices could be empty.
			final DataSet<Tuple2<Long, Long>> repeatedExpansionVertices2 = positiveHops2
					.join(outVertices2)
					.where(0)
					.equalTo(0)
					.with(new RepeatedExpansionNormalizer()) // repeated vertices with a lower score than original will have score set to -1
					.name("DeltaIteration positiveHops RepeatedExpansionNormalizer");
			System.out.println("repeatedExpansionVertices");
			repeatedExpansionVertices2.print();

			// nextWorkset contains new vertices included in the expansion and old vertices whose propagated expansion hop value is greater than the original value in the current workset.
			final DataSet<Tuple2<Long, Long>> nextWorkset2 = outVertices2 // outVertices has no repeated keys
					.union(repeatedExpansionVertices2) // repeatedExpansionVertices has no repeated keys
					.groupBy(0)
					.reduce(new RepeatedVertexReducer())  // if a group of 2 repeated vertices has a vertex with score -1L, we retain the -1L score
					.filter(new KeepRanksAbove(-1L)) //remove negative scores.
					.name("DeltaIteration outVertices RepeatedVertexReducer");
			System.out.println("nextWorkset");
			nextWorkset2.print();


			final DataSet<Tuple2<Long, Long>> delta2 = nextWorkset2
					.filter(new FilterFunction<Tuple2<Long, Long>>() { // include the newly-reached vertices with hop count = 0
						@Override
						public boolean filter(Tuple2<Long, Long> longLongTuple2) throws Exception {
							return longLongTuple2.f1 == 0;
						}
					})
					.union(nextWorkset)
					.name("DeltaIteration nextWorkSet -> delta");
			System.out.println("delta");
			delta2.print();

			//final DataSet<Long> expandedVertices = deltaExpansionSimplified(expandedIds, g, iterationLimit);
		}
		catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}

		//final DataSet<Long> expandedVertices = deltaExpansion(expandedIds, graph, iterationLimit);
		//TESTING
		Assertions.assertEquals(true, true);


		//System.out.println(=)
	}
*/
	/**
	 * Test the GraphBolt hot vertex set calculation with n=1 after the first set of updates is applied.
	 * @param testInfo
	 */
	@Test
	@DisplayName("testStep1ParameterExpansionInfoDataSetN1")
	void testStep1ParameterExpansionInfoDataSetN1(final TestInfo testInfo) {
		try {
			final GraphBoltTest instance = getInstance();
			final ExecutionEnvironment env = instance.getContext();

			// Get vertices where the previous in-degree was 0 or it changed enough so that abs((curr-degree / prev-degree) - 1) > vertexDegreeMininumChangeRatio
			final Double r = new Double(0.05);
			final Map<Long, UpdateInfo> infos = instance.getUpdateTracker().getUpdateInfos();

			/*
			final DataSet<Tuple2<Long, UpdateInfo>> infoDataSet = ApproximatePageRank.infoMapToDataSet(infos, env);
			final DataSet<Tuple3<Long, Double, Long>> kHotVertices = BigVertexGraph.getKHotVertices(infoDataSet, r, EdgeDirection.IN);

			final DataSet<Tuple2<Long, Double>> previousRanks = instance.getOfflineResults().get(0);
			if(instance.debugging()) {

				// Print the UpdateInfo items corresponding to the initial graph.
				System.out.println("infos: ");
				for(Long u : infos.keySet()) {
					System.out.println(u + "\t " + infos.get(u));
				}
				System.out.println("infoDataSet: ");
				infoDataSet.print();

				// Ranks from the previous PageRank Python power-method execution.
				System.out.println("previousRanks: ");
				previousRanks.print();


				System.out.println("kHotVertices: ");
				kHotVertices.print();
			}

			//TODO: mudar o nome de cada variável de DataSet para ser mais percetível o que se está a passar.

			// Set of vertices whose degree change is equal or more than vertexDegreeMininumChangeRatio: 6, 7, 10
			// Included in those sets are the vertices which are new: 12, 13, 14

			Assertions.assertEquals(6, kHotVertices.count(), () -> "The kHotVertices DataSet is supposed to finish with six (6) vertices.");
			*/
			/*
			kHotVertices
			(VERTEX ID, DEGREE UPDATE RATIO, CURRENT IN DEGREE)
			(7, 1.0, 2)
			(6, 1.0, 2)
			(10, Infinity, 1) // had a previous degree of 0
			(13, Infinity, 0) // newly-added
			(14, Infinity, 1) // newly-added
			(12, Infinity, 2) // newly-added
			Note: vertices which have been newly-added or had a previous in-degree of 0 have ratio = Infinity
			*/

			/*
			double avgPrevDegree = BigVertexGraph.getPreviousAvgInDegree(infoDataSet);
			final Integer n = 1;
			final Double delta = new Double(0.0400d);
			final DataSet<Tuple2<Long, Long>> expandedIds = BigVertexGraph.expandKHotVertices(kHotVertices, previousRanks, n, delta, avgPrevDegree);

			if(getInstance().debugging()) {
				System.out.println("expandedIds: ");
				expandedIds.print();
			}
			Assertions.assertEquals(6, expandedIds.count(), () -> "The expandedIds DataSet is supposed to finish with six (6) vertices.");

			*/

			/*
			expandedIds
			(VERTEX ID, NUMBER OF HOPS TO MOVE FROM VERTEX)
			(7,2)
			(13,1)
			(6,2)
			(12,1)
			(10,1)
			(14,1)
			 */

			/*
			Integer iterationLimit = expandedIds.max(1).collect().get(0).f1.intValue();
			System.out.println("Delta iteration limit: " + iterationLimit.toString());
			DataSet<Long> results = BigVertexGraph.deltaExpansion(expandedIds, getInstance().getGraphList().get(1), iterationLimit);

			List<Long> resultsList = results.collect();

			if(getInstance().debugging()) {
				for (Long t : resultsList) {
					System.out.println(t);
				}
			}

			*/
			/* Vertices included after the delta iteration.
			resultsList
			 (0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 12, 13, 14)
			  */

			//Assertions.assertEquals(13, resultsList.size(), () -> "The expanded vertex set is supposed to have thirteen (13) vertices.");

		} catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
	}

	/**
	 * Test the GraphBolt hot vertex set calculation with n=1 after the first set of updates is applied.
	 * This is using a revised version of DeltaIteration because it was consuming huge amounts of memory on Cit-HepPh and Facebook-links, leading to all threads being stuck on waiting.
	 * @param testInfo
	 */
	@Test
	@DisplayName("testStep1ParameterExpansionInfoDataSetNewDeltaIterationN1")
	void testStep1ParameterExpansionInfoDataSetNewDeltaIterationN1(final TestInfo testInfo) {
		try {
			final GraphBoltTest instance = getInstance();
			final ExecutionEnvironment env = instance.getContext();

			// Get vertices where the previous in-degree was 0 or it changed enough so that abs((curr-degree / prev-degree) - 1) > vertexDegreeMininumChangeRatio

			/*
			final Double r = new Double(0.05);
			final Map<Long, UpdateInfo> infos = instance.getUpdateTracker().getUpdateInfos();
			final DataSet<Tuple2<Long, UpdateInfo>> infoDataSet = ApproximatePageRank.infoMapToDataSet(infos, env);
			final DataSet<Tuple3<Long, Double, Long>> kHotVertices = BigVertexGraph.getKHotVertices(infoDataSet, r, EdgeDirection.IN);

			final DataSet<Tuple2<Long, Double>> previousRanks = instance.getOfflineResults().get(0);
			if(instance.debugging()) {

				// Print the UpdateInfo items corresponding to the initial graph.
				System.out.println("infos: ");
				for(Long u : infos.keySet()) {
					System.out.println(u + "\t " + infos.get(u));
				}
				System.out.println("infoDataSet: ");
				infoDataSet.print();

				// Ranks from the previous PageRank Python power-method execution.
				System.out.println("previousRanks: ");
				previousRanks.print();


				System.out.println("kHotVertices: ");
				kHotVertices.print();
			}
*/
			//TODO: mudar o nome de cada variável de DataSet para ser mais percetível o que se está a passar.

			// Set of vertices whose degree change is equal or more than vertexDegreeMininumChangeRatio: 6, 7, 10
			// Included in those sets are the vertices which are new: 12, 13, 14

			//Assertions.assertEquals(6, kHotVertices.count(), () -> "The kHotVertices DataSet is supposed to finish with six (6) vertices.");
			/*
			kHotVertices
			(VERTEX ID, DEGREE UPDATE RATIO, CURRENT IN DEGREE)
			(7, 1.0, 2)
			(6, 1.0, 2)
			(10, Infinity, 1) // had a previous degree of 0
			(13, Infinity, 0) // newly-added
			(14, Infinity, 1) // newly-added
			(12, Infinity, 2) // newly-added
			Note: vertices which have been newly-added or had a previous in-degree of 0 have ratio = Infinity
			*/
/*
			double avgPrevDegree = BigVertexGraph.getPreviousAvgInDegree(infoDataSet);
			final Integer n = 1;
			final Double delta = new Double(0.0400d);
			final DataSet<Tuple2<Long, Long>> expandedIds = BigVertexGraph.expandKHotVertices(kHotVertices, previousRanks, n, delta, avgPrevDegree);

			if(getInstance().debugging()) {
				System.out.println("expandedIds: ");
				expandedIds.print();
			}
			Assertions.assertEquals(6, expandedIds.count(), () -> "The expandedIds DataSet is supposed to finish with six (6) vertices.");

			*/
			/*
			expandedIds
			(VERTEX ID, NUMBER OF HOPS TO MOVE FROM VERTEX)
			(7,2)
			(13,1)
			(6,2)
			(12,1)
			(10,1)
			(14,1)
			 */

/*
			Integer iterationLimit = expandedIds.max(1).collect().get(0).f1.intValue();
			System.out.println("Delta iteration limit: " + iterationLimit.toString());
			DataSet<Long> results = BigVertexGraph.deltaExpansionSimplified(expandedIds, getInstance().getGraphList().get(1), iterationLimit, env);

			List<Long> resultsList = results.collect();

			if(getInstance().debugging()) {
				for (Long t : resultsList) {
					System.out.println(t);
				}
			}
			+/


			/* Vertices included after the delta iteration.
			resultsList
			 (0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 12, 13, 14)
			  */

			//Assertions.assertEquals(13, resultsList.size(), () -> "The expanded vertex set is supposed to have thirteen (13) vertices.");

		} catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
	}
	
	private static final class EdgeRemovalCoGroup<K, EV> implements CoGroupFunction<Edge<K, EV>, Edge<K, EV>, Edge<K, EV>> {

		private static final long serialVersionUID = 2791464822632107942L;

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

	@Test
	@DisplayName("testStep1EdgeCount")
	void testStep1EdgeCount(final TestInfo testInfo) {
		try {
			final GraphBoltTest instance = getInstance();
			final long numberOfEdges = instance.getGraphList().get(1).numberOfEdges();
			Assertions.assertEquals(instance.getEdgeCountList().get(1).longValue(), numberOfEdges, () -> "Edge count mismatch.");
		} catch (final Exception e) {
			Assertions.fail(e.getMessage());
		}
	}
	
	/**
	 * Compare networkx Python power-method PageRank order to the SimplePageRank implementation targeting Apache Flink.
	 * Applies to step 0 of the graph.
	 * @param testInfo
	 */
	/*
	@Test
	@DisplayName("testStep0PowerRankSimplePageRank")
	void testStep0PowerRankSimplePageRank(final TestInfo testInfo) {
		try {
			getInstance().getContext().getConfig().setParallelism(1);
			
			// Convert the graph vertex value type from NullValue to Double.
			final Graph<Long, Double, NullValue> doublesGraph = getInstance().getGraphList().get(0)
					.mapVertices(new GraphDoubleInitializer<Long, NullValue>());
			
			// Compute GraphBolt's SimplePageRank.
			final SimplePageRank<Long, Double, NullValue> simplePageRank = new SimplePageRank<>(0.85d, null, 300);
			final DataSet<Tuple2<Long, Double>> simpleRanks = simplePageRank
					.run(doublesGraph)
					.sortPartition(1, Order.DESCENDING);

			final DataSet<Tuple2<Long, Double>> precomputedRanks = getInstance().getOfflineResults().get(0);
			
			if(getInstance().debugging()) {
				//LOG.log(level, msg, thrown);
				//LOG.in
				//System.out.println("Python networkx power-method ranks:");
				
				LOG.info("Python networkx power-method ranks:");
				precomputedRanks.print();
				//System.out.println("Apache Flink SimplePageRank method ranks:");
				LOG.info("Apache Flink SimplePageRank method ranks:");
				simpleRanks.print();
			}
			
			// The SimplePageRank of GraphBolt compared to networkx power-method yields an RBO of 0.99.
			final double rbo = RBO(precomputedRanks, simpleRanks, +0.98d);
			
			if(getInstance().debugging()) {
				System.out.println("GraphBolt SimplePageRank rbo: " + rbo);
			}
			
			Assertions.assertEquals(0.99, rbo, 0.1d, () -> "RBO mismatch.");
		} catch (final Exception e) {
			Assertions.fail(e.toString());
		}
	}
	*/
	
	/**
	 * Compare networkx Python power-method PageRank order to the SimplePageRank implementation targeting Apache Flink.
	 * Applies to step 1 of the graph.
	 * @param testInfo
	 */
	/*
	@Test
	@DisplayName("testStep1PowerRankSimplePageRank")
	void testStep1PowerRankSimplePageRank(final TestInfo testInfo) {
		try {
			getInstance().getContext().getConfig().setParallelism(1);
			
			// Convert the graph vertex value type from NullValue to Double.
			final Graph<Long, Double, NullValue> doublesGraph = getInstance().getGraphList().get(1)
					.mapVertices(new GraphDoubleInitializer<Long, NullValue>());
			
			// Compute GraphBolt's SimplePageRank.
			//final SimplePageRank<Long, Double, NullValue> simplePageRank = new SimplePageRank<>(0.85d, null, 100);
			final SimplePageRank<Long, Double, NullValue> simplePageRank = new SimplePageRank<>(0.85d, 1.0d / doublesGraph.numberOfVertices(), 100);
			final DataSet<Tuple2<Long, Double>> simpleRanks = simplePageRank
					.run(doublesGraph)
					.sortPartition(1, Order.DESCENDING)
					.setParallelism(1);

			List<Tuple2<Long, Double>> simpleRanksList = simpleRanks.collect();

			final DataSet<Tuple2<Long, Double>> precomputedRanks = getInstance()
					.getOfflineResults()
					.get(1)
					.sortPartition(1, Order.DESCENDING)
					.setParallelism(1);

			List<Tuple2<Long, Double>> precomputedRanksList = precomputedRanks.collect();

			if(getInstance().debugging()) {
				System.out.println("Python networkx power-method ranks:");
				precomputedRanks.print();
				System.out.println("Apache Flink SimplePageRank method ranks:");
				simpleRanks.print();
			}
			
			// The SimplePageRank of GraphBolt compared to networkx power-method yields an RBO of 0.99.
			final double rbo = RBO(precomputedRanks, simpleRanks, +0.98d);
			
			if(getInstance().debugging()) {
				System.out.println("GraphBolt SimplePageRank rbo: " + rbo);
			}
			
			Assertions.assertEquals(0.99, rbo, 0.1d, () -> "RBO mismatch.");
		} catch (final Exception e) {
			Assertions.fail(e.toString());
		}
	}
*/
	private static class RankInitializer implements MapFunction<Vertex<Long, Double>, Double> {

		private static final long serialVersionUID = -7897516140927538909L;
		Long vertexCount;

		public RankInitializer(Long vertexCount) {
			this.vertexCount = vertexCount;
		}

		@Override
		public Double map(Vertex<Long, Double> longDoubleVertex) throws Exception {
			//return longDoubleVertex.f1 / vertexCount;
			return 1.0d;
		}
	}

	private static class EdgeInitializer implements MapFunction<Edge<Long, NullValue>, Double> {
		private static final long serialVersionUID = -9077584184213995731L;

		@Override
		public Double map(Edge<Long, NullValue> value) throws Exception {
			return 1.0d;
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

	@SuppressWarnings("serial")
	private static final class InitMapper implements MapFunction<Long, Double> {
		public Double map(Long value) {
			return 1.0;
		}
	}

	@Test
	@DisplayName("FlinkPageRankDataTest")
	void FlinkPageRankDataTest(final TestInfo testInfo) {

		ExecutionEnvironment env = getInstance().getContext();

		env.getConfig().setParallelism(1);

		Graph<Long, Double, Double> inputGraph = Graph.fromDataSet(
				PageRankData.getDefaultEdgeDataSet(env), new InitMapper(), env);

		try {

			org.apache.flink.graph.library.linkanalysis.PageRank<Long, Double, Double> pr = new org.apache.flink.graph.library.linkanalysis.PageRank<Long, Double, Double>(0.85d, 100, 1E-10);
			DataSet<Result<Long>> result = pr.setParallelism(1).run(inputGraph);
			System.out.println("org.apache.flink.graph.library.linkanalysis.PageRank");
			for(Result<Long> v : result.collect()) {
				System.out.println(v);
			}

			System.out.println("org.apache.flink.graph.examples.PageRank");
			final DataSet<Vertex<Long, Double>> simpleRanks = new org.apache.flink.graph.examples.PageRank<Long>(0.85d, 100).run(inputGraph);
			//List<Vertex<Long, Double>> simpleRanksList = simpleRanks.collect();
			simpleRanks.sortPartition(1, Order.DESCENDING).setParallelism(1).print();

			Assertions.assertEquals(1, 1, () -> "RBO mismatch.");
		} catch (final Exception e) {
			Assertions.fail(e.toString());
		}
	}

	/**
	 * Compare networkx Python power-method PageRank order to the SimplePageRank implementation targeting Apache Flink.
	 * Applies to step 1 of the graph.
	 * @param testInfo
	 */
/*
	@Test
	@DisplayName("testStep1PowerRankFlinkPageRank")
	void testStep1PowerRankFlinkPageRank(final TestInfo testInfo) {
		try {
			getInstance().getContext().getConfig().setParallelism(1);

			/*
			// Convert the graph vertex value type from NullValue to Double.
			final Graph<Long, Double, NullValue> doublesGraph = getInstance().getGraphList().get(1)
					.mapVertices(new GraphDoubleInitializer<Long, NullValue>());

			// Compute GraphBolt's SimplePageRank.
			//final SimplePageRank<Long, Double, NullValue> simplePageRank = new SimplePageRank<>(0.85d, null, 100);
			/*final SimplePageRank<Long, Double, NullValue> simplePageRank = new SimplePageRank<>(0.85d, 1.0d / doublesGraph.numberOfVertices(), 100);
			final DataSet<Tuple2<Long, Double>> simpleRanks = simplePageRank
					.run(doublesGraph)
					.sortPartition(1, Order.DESCENDING)
					.setParallelism(1); */

			//DataSet<Tuple2<Long, LongValue>> vertexOutDegrees = doublesGraph.outDegrees();
/*
			long vertexCount = doublesGraph.numberOfVertices();
			Graph<Long, Double, Double> g = doublesGraph
					.mapVertices(new RankInitializer(vertexCount))
					.mapEdges(new EdgeInitializer());

					//.joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

			final DataSet<Vertex<Long, Double>> simpleRanks = new PageRank<Long>(0.85d, 100).run(g);
			List<Vertex<Long, Double>> simpleRanksList = simpleRanks.collect();

			final DataSet<Tuple2<Long, Double>> precomputedRanks = getInstance()
					.getOfflineResults()
					.get(1)
					.sortPartition(1, Order.DESCENDING)
					.setParallelism(1);

			List<Tuple2<Long, Double>> precomputedRanksList = precomputedRanks.collect();

			if(getInstance().debugging()) {
				System.out.println("Python networkx power-method ranks:");
				precomputedRanks.print();
				System.out.println("Apache Flink SimplePageRank method ranks:");
				simpleRanks.print();
			}

			// The SimplePageRank of GraphBolt compared to networkx power-method yields an RBO of 0.99.

			DataSet<Tuple2<Long, Double>> convertedRanks = simpleRanks
					.map(new MapFunction<Vertex<Long, Double>, Tuple2<Long, Double>>() {
						@Override
						public Tuple2<Long, Double> map(Vertex<Long, Double> longDoubleVertex) throws Exception {
							return Tuple2.of(longDoubleVertex.f0, longDoubleVertex.f1);
						}
					});

			final double rbo = RBO(precomputedRanks, convertedRanks, +0.98d);

			if(getInstance().debugging()) {
				System.out.println("GraphBolt SimplePageRank rbo: " + rbo);
			}

			Assertions.assertEquals(0.99, rbo, 0.1d, () -> "RBO mismatch.");
		} catch (final Exception e) {
			Assertions.fail(e.toString());
		}
	}
	*/
	@Test
	@DisplayName("testRBO")
	void testRBO(final TestInfo testInfo) {
		
		List<Tuple2<Long, Double>> S = Arrays.asList(
				Tuple2.of(0L, -1d),
				Tuple2.of(1L, -1d),
				Tuple2.of(2L, -1d),
				Tuple2.of(3L, -1d),
				Tuple2.of(4L, -1d),
				Tuple2.of(5L, -1d),
				Tuple2.of(6L, -1d));
		
		List<Tuple2<Long, Double>> L = Arrays.asList(
				Tuple2.of(1L, -1d),
				Tuple2.of(0L, -1d),
				Tuple2.of(2L, -1d),
				Tuple2.of(3L, -1d),
				Tuple2.of(4L, -1d),
				Tuple2.of(5L, -1d),
				Tuple2.of(7L, -1d));
		
		final double rbo = RBO(S, L, +0.9800000000);
		Assertions.assertEquals(0.853451088448, rbo, () -> "RBO mismatch.");
	}
	
	
	
	/**
	 * Linear implementation of the Ranked Biased Overlap (RBO) score which assumes there are no duplicates and doesn't handle for ties.
	 * Wrapper for the RBO function with debugging disabled.
	 * 
	 * @see https://github.com/ragrawal/measures/blob/master/measures/rankedlist/RBO.py
	 * 
	 * @param S a list of vertex ids.
	 * @param L a list of vertex ids (should be longer than S)
	 * @param p user persistence, the probability to continue observing list elements
	 * @return RBO, the expected value of the list overlap as a double value between 0 and 1
	 * 
	 * @authors Miguel E. Coimbra
	 */
	private static double RBO(final List<Long> S, final List<Long> L, final Double p) {
		return RBO(S, L, p, false);
	}
	

	/**
	 * Linear implementation of the Ranked Biased Overlap (RBO) score which assumes there are no duplicates and doesn't handle for ties.
	 * 
	 * @see https://github.com/ragrawal/measures/blob/master/measures/rankedlist/RBO.py
	 * 
	 * @param S a list of vertex ids.
	 * @param L a list of vertex ids (should be longer than S)
	 * @param p user persistence, the probability to continue observing list elements
	 * @return RBO, the expected value of the list overlap as a double value between 0 and 1
	 * 
	 * @authors Miguel E. Coimbra
	 */
	private static double RBO(List<Long> S, List<Long> L, final Double p, final boolean debugging) {
		
		// Sanity checks.
		if(p.isNaN())
			throw new IllegalArgumentException("p cannot be a NaN");
		else if(p.isInfinite())
			throw new IllegalArgumentException("p cannot be infinite");
		else if(p.intValue() < 0 || p.intValue() > 1)
			throw new IllegalArgumentException("p must be between 0 and 1");
		else if(S == null) 
			throw new IllegalArgumentException("S can't be null");
		else if(L == null) 
			throw new IllegalArgumentException("L can't be null");
		
		
		// Ensure that both lists are not null and that S is the shorter list and L the longer one in case they differ in size.
		if (L.size() < S.size()) {
			final List<Long> aux = S;
			S = L;
			L = aux;	
		}
		
		final int l = L.size();
		final int s = S.size();
		
		// If the (potentially) smaller list is empty, RBO is 0.
		if(s == 0)
			return 0;
		
		final HashMap<Integer, Double> x_d = new HashMap<Integer, Double>();
		x_d.put(0, +0.0000000000d);
		double sum1 = +0.0000000000d;
		
		
		final Set<Long> ss = new HashSet<Long>();
		final Set<Long> ls = new HashSet<Long>();
		
		if(getInstance().debugging()) {
			for(final Long e : S) {
				System.out.print(e + ", ");
			}
			
			for(final Long e : L) {
				System.out.print(e + ", ");
			}
		}
		
		
		
		for(int i = 0; i < s; i++) {
			final Long x = L.get(i);
			final Long y = (i < s) ? S.get(i) : null;
			final int d = i + 1;
			
			//if(getInstance().debugging())
			//	System.out.println(String.format("i: %s\tx: %s\ty: %s", new Integer(i).toString(), x.toString(), y.toString()));
			
			// If two elements are same then we don't need to add to either of the sets.
			if (x == y) {
				x_d.put(d, x_d.get(d-1) + 1.0);
			}
			else {
				// Add items to respective list and calculate the overlap.
				ls.add(x);
				if (y != null) {
					ss.add(y);
				}
				
				x_d.put(d, x_d.get(d-1) + (ss.contains(x) ? 1.0 : 0.0) + (ls.contains(y) ? 1.0 : 0.0));
			}
			// Calculate average overlap.
			sum1 += x_d.get(d) / d * Math.pow(p, d);
		}
		
		double sum2 = +0.0d;
		for(int i = 0, lim = l - s; i < lim; i++) {
			final int d = s + i + 1;
			sum2 += x_d.get(d) * (d - s) / (d * s) * Math.pow(p, d);
		}
		
		final double sum3 = ((x_d.get(l) - x_d.get(s)) / l + x_d.get(s) / s) * Math.pow(p, l);
		
		// Equation 32
		final double rbo_ext = (1 - p) / p * (sum1 + sum2) + sum3;
		
		
		if(getInstance().debugging()) {
			System.out.println("sum 1 " + sum1 + " sum2 " + sum2 + " sum3 " + sum3);
			System.out.println("SET ls");
			
			for(final Long e : ls) {
				System.out.println(e);
			}
			
			System.out.println("SET ss");
			
			for(final Long e : ss) {
				System.out.println(e);
			} 
		}
		
		return rbo_ext;
	}
	
	/**
	 * Linear implementation of the Ranked Biased Overlap (RBO) score which assumes there are no duplicates and doesn't handle for ties.
	 * This is a wrapper for the RBO static method that receives List<Tuple2<Long, Double>>.
	 * This is based on a modified implementation.
	 * @see https://github.com/ragrawal/measures/blob/master/measures/rankedlist/RBO.py
	 * 
	 * @param dataSetS a Flink DataSet of vertex ids and their ranks.
	 * @param dataSetL a Flink DataSet of vertex ids and their ranks. (should be equal or greater than S in size)
	 * @param p user persistence, the probability to continue observing list elements
	 * @return RBO, the expected value of the list overlap as a double value between 0 and 1
	 * 
	 * @authors Miguel E. Coimbra
	 */
	private static double RBO(DataSet<Tuple2<Long, Double>> dataSetS, DataSet<Tuple2<Long, Double>> dataSetL, double p) {
		List<Tuple2<Long, Double>> tupleListS = null;
		List<Tuple2<Long, Double>> tupleListL = null;
		
		try {
			tupleListS = dataSetS.collect();
			tupleListL = dataSetL.collect();
		} catch (final Exception e) {
			Assertions.fail(e.toString());
		}
		
		return RBO(tupleListS, tupleListL, p);
	}
	
	/**
	 * Linear implementation of the Ranked Biased Overlap (RBO) score which assumes there are no duplicates and doesn't handle for ties.
	 * This is a wrapper for the RBO static method that receives List<Long>.
	 * This is based on a modified implementation.
	 * @see https://github.com/ragrawal/measures/blob/master/measures/rankedlist/RBO.py
	 * 
	 * @param tupleListS a Java list of vertex ids as Long values.
	 * @param tupleListL a java list of vertex ids as Long values. (should be equal or greater than S in size)
	 * @param p user persistence, the probability to continue observing list elements
	 * @return RBO, the expected value of the list overlap as a double value between 0 and 1
	 * 
	 * @authors Miguel E. Coimbra
	 */
	private static double RBO(List<Tuple2<Long, Double>> tupleListS, List<Tuple2<Long, Double>> tupleListL, double p) {
		final List<Long> listS = new ArrayList<Long>(tupleListS.size());
		for(final Tuple2<Long, Double> vertexTuple : tupleListS) {
			listS.add(vertexTuple.f0);
		}
		
		final List<Long> listL = new ArrayList<Long>(tupleListL.size());
		for(final Tuple2<Long, Double> vertexTuple : tupleListL) {
			listL.add(vertexTuple.f0);
		}
		
		return RBO(listS, listL, p);
	}
		
	
	
	
	@Test
	@DisplayName("testRBOpersistenceIsNan")
	void testRBOpersistenceIsNan(final TestInfo testInfo) {
		final IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            RBO(Arrays.asList(new Long(0L)), 
            	Arrays.asList(new Long(1L)),
            	Double.NaN);
            });
		
		Assertions.assertEquals("p cannot be a NaN", exception.getMessage());
	}
	
	@Test
	@DisplayName("testRBOpersistenceIsPositiveInfinity")
	void testRBOpersistenceIsPositiveInfinity(final TestInfo testInfo) {
		final IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            RBO(Arrays.asList(new Long(0L)), 
            	Arrays.asList(new Long(1L)),
            	Double.POSITIVE_INFINITY);
            });
		
		Assertions.assertEquals("p cannot be infinite", exception.getMessage());
	}
	
	@Test
	@DisplayName("testRBOpersistenceIsNegativeInfinity")
	void testRBOpersistenceIsNegativeInfinity(final TestInfo testInfo) {
		final IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            RBO(Arrays.asList(new Long(0L)), 
            	Arrays.asList(new Long(1L)),
            	Double.NEGATIVE_INFINITY);
            });
		
		Assertions.assertEquals("p cannot be infinite", exception.getMessage());
	}
	
	@Test
	@DisplayName("testRBOPersistenceIsNegative")
	void testRBOPersistenceIsNegative(final TestInfo testInfo) {
		final IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            RBO(Arrays.asList(new Long(0L)), 
            	Arrays.asList(new Long(1L)),
            	-1.0d);
            });
		
		Assertions.assertEquals("p must be between 0 and 1", exception.getMessage());
	}
	
	@Test
	@DisplayName("testRBOPersistenceIsAboveOne")
	void testRBOPersistenceIsAboveOne(final TestInfo testInfo) {
		final IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            RBO(Arrays.asList(new Long(0L)), Arrays.asList(new Long(1L)), 50.0d);
        });
		
		Assertions.assertEquals("p must be between 0 and 1", exception.getMessage());
	}
	
	@Test
	@DisplayName("testRBOnullListS")
	void testRBOnullListS(final TestInfo testInfo) {
		final IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            RBO(null, Arrays.asList(new Long(1L)), 0.5d);
        });
		
		Assertions.assertEquals("S can't be null", exception.getMessage());
	}
	
	@Test
	@DisplayName("testRBOnullListL")
	void testRBOnullListL(final TestInfo testInfo) {
		final IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            RBO(Arrays.asList(new Long(1L)), null, 0.5d);
        });
		
		Assertions.assertEquals("L can't be null", exception.getMessage());
	}
}
