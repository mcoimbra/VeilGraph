package pt.ulisboa.tecnico.graph.stream;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Abstract stream provider
 *
 * @param <T>
 */
public abstract class StreamProvider<T> implements Runnable {
    protected final BlockingQueue<T> queue;

    public StreamProvider() {
        this.queue = new LinkedBlockingQueue<>();
    }

    public BlockingQueue<T> getQueue() {
        return this.queue;
    }
}
