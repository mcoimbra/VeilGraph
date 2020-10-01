package pt.ulisboa.tecnico.veilgraph.stream;

import org.apache.flink.api.java.tuple.Tuple7;

/**
 * Container for graph statistics
 *
 * @author Renato Rosa
 */
public class GraphUpdateStatistics extends Tuple7<Long, Long, Long, Long, Long, Long, Long> {
	private static final long serialVersionUID = 5621532845594775435L;

	public GraphUpdateStatistics(final long addedVertices, final long removedVertices, final long addedEdges, 
			final long removedEdges, final long updatedVertices, final long totalVertices, final long totalEdges) {
        this.f0 = addedVertices;
        this.f1 = removedVertices;
        this.f2 = addedEdges;
        this.f3 = removedEdges;
        this.f4 = updatedVertices;
        this.f5 = totalVertices;
        this.f6 = totalEdges;
    }

    public long getAddedVertices() {
        return f0;
    }

    public long getRemovedVertices() {
        return f1;
    }

    public long getAddedEdges() {
        return f2;
    }

    public long getRemovedEdges() {
        return f3;
    }

    public long getUpdatedVertices() {
        return f4;
    }

    public long getTotalVertices() {
        return f5;
    }

    public long getTotalEdges() {
        return f6;
    }
}
