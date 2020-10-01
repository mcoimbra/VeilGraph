package pt.ulisboa.tecnico.veilgraph.stream;

import org.apache.flink.graph.Edge;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Container for graph updates
 *
 * @param <K>
 * @param <EV>
 * @author Renato Rosa
 */
public class GraphUpdates<K, EV> implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = -821352704978401270L;
	public final Set<K> verticesToAdd;
    public final Set<K> verticesToRemove;
    public final Set<Edge<K, EV>> edgesToAdd;
    public final Set<Edge<K, EV>> edgesToRemove;

    GraphUpdates(final Set<K> verticesToAdd, final Set<K> verticesToRemove, final Set<Edge<K, EV>> edgesToAdd, final Set<Edge<K, EV>> edgesToRemove) {
        this.verticesToAdd = new HashSet<>(verticesToAdd);
        this.verticesToRemove = new HashSet<>(verticesToRemove);
        this.edgesToAdd = new HashSet<>(edgesToAdd);
        this.edgesToRemove = new HashSet<>(edgesToRemove);
    }
}
