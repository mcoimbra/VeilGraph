package pt.ulisboa.tecnico.graph.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.ulisboa.tecnico.graph.stream.GraphUpdateTracker;


/**
 *
 * @param <K> vertex key type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @param <R> algorithm return type
 */
public interface GraphModel<K, VV, EV, R> {



    @Override
    String toString();

    void initStatistics(final String statisticsDirectory);

    void registerStatistics(final Long iteration, final ExecutionEnvironment env) throws Exception;

    void setModelDirectory(final String modelDirectory);

    void cleanup();

    //  protected abstract boolean checkUpdateState(final GraphUpdates<Long, NullValue> updates, final GraphUpdateStatistics statistics);


    Graph<K, Double, Double> getGraph(final ExecutionEnvironment env, Graph<K, NullValue, NullValue> graph, final DataSet<Tuple2<K, GraphUpdateTracker.UpdateInfo>> infoDataSet, DataSet<R> previousResults) throws Exception;

    Long getSetupTime();


}
