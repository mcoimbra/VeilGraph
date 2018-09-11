package pt.ulisboa.tecnico.graph.algorithm.avgdegree;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.util.Collector;

/**
 * Algorithm to calculate the average degree of a graph
 *
 * @param <K>
 * @param <VV>
 * @param <EV>
 * @author Renato Rosa
 */
public class AverageDegree<K, VV, EV> implements GraphAlgorithm<K, VV, EV, DataSet<Double>> {

    @Override
    public DataSet<Double> run(final Graph<K, VV, EV> graph) throws Exception {
    	final TypeHint<Tuple2<Long, Integer>> tuple2TypeHint = new TypeHint<Tuple2<Long, Integer>>() {};
        return graph.outDegrees()
                .map(t -> Tuple2.of(t.f1.getValue(), 1))
                .returns(tuple2TypeHint)
                .reduce((t1, t2) -> Tuple2.of(t1.f0 + t2.f0, t1.f1 + t2.f1))
                .returns(tuple2TypeHint)
                .flatMap((Tuple2<Long, Integer> v, Collector<Double> out) ->
                        out.collect(v.f0.doubleValue() / v.f1.doubleValue()));
    }
}
