package pt.ulisboa.tecnico.veilgraph.stream;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import java.util.Map;

/**
 * Callback methods to be defined by the user
 *
 * @param <K>
 * @param <EV>
 * @author Renato Rosa
 */
public interface StreamObserver<K, EV> {
    void onStart() throws Exception;

    /**
     * User-defined function. Allows the user to decide if updates must be applied to the graph or delayed.
     * It is invoked in an implementation of GraphStreamHandler<Tuple2<Long, Double>> such as ApproximatePageRank.
     * Invoked when an edge addition/removal arrives as an update.
     *
     * @param updates
     * @param statistics
     * @return
     */
    //boolean checkUpdateState(final GraphUpdates<K, EV> updates, final GraphUpdateStatistics statistics);


    /**
     * User-defined function. The user implementing this function must consider the cost-benefit relation when implementing this function.
     *
     * @param id The identifier of the query.
     * @param query The query itself.
     * @param graph
     * @param updates
     * @param statistics
     * @param updateInfos
     * @return
     */
    GraphStreamHandler.Action onQuery(
            final int id,
            final String query,
            final Graph<Long, NullValue, NullValue> graph,
            final GraphUpdates<K, EV> updates,
            final GraphUpdateStatistics statistics,
            final Map<K, GraphUpdateTracker.UpdateInfo> updateInfos);

    /**
     *
     *
     * @param id
     * @param query
     * @param action
     * @param graph
     * @param summaryGraph
     * @param result
     * @param jobExecutionResult
     */
    void onQueryResult(
            final int id,
            final String query,
            final GraphStreamHandler.Action action,
            final Graph<Long, NullValue, NullValue> graph,
            final Graph<Long, Double, Double> summaryGraph,
            final DataSet<Tuple2<Long, Double>> result,
            final JobExecutionResult jobExecutionResult,
            final int iterations);

    void onStop() throws Exception;

//    void printCompleteTime(long accumulatedTimeBeforeApplyingUpdate, long updateApplicationTim);
//	void printApproxTime(long accumulatedTimeBeforeApplyingUpdate, long updateApplicationTime, long paramSetup, long approxComputationTime);
}
