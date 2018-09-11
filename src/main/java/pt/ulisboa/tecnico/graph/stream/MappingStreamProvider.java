package pt.ulisboa.tecnico.graph.stream;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * An abstract {@link StreamProvider} that transforms its input data
 *
 * @param <T>
 * @param <V>
 */
public abstract class MappingStreamProvider<T, V> extends StreamProvider<V> {

    private final MapFunction<T, V> mapFunction;

    public MappingStreamProvider(MapFunction<T, V> mapFunction) {
        this.mapFunction = mapFunction;
    }

    protected void mapAndPut(T el) throws Exception {
        super.queue.put(this.mapFunction.map(el));
    }
}
