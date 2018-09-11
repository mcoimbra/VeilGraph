package pt.ulisboa.tecnico.graph.stream;

import java.util.Collection;

/**
 * Stream provider for elements coming from a java {@link Collection}
 *
 * @param <T>
 * @author Renato Rosa
 */
public class CollectionStreamProvider<T> extends StreamProvider<T> {
    private final Collection<T> collection;

    public CollectionStreamProvider(final Collection<T> collection) {
        this.collection = collection;
    }

    @Override
    public void run() {
        super.queue.addAll(collection);
    }
}
