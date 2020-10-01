package pt.ulisboa.tecnico.veilgraph.validation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.validation.GraphValidator;

public class LongPositiveIdsValidator<K, VV, EV> extends GraphValidator<K, VV, EV> {

    /**
	 * 
	 */
	private static final long serialVersionUID = -5093552519027221038L;

	private static TypeInformation<Long> longTypeInfo = TypeInformation.of(Long.class);

    private DataSet<K> offendingIds;

    @Override
    public boolean validate(final Graph<K, VV, EV> graph) throws Exception {
        final DataSet<K> vertexIds = graph.getVertexIds();

        if (!vertexIds.getType().equals(LongPositiveIdsValidator.longTypeInfo)) {
            this.offendingIds = vertexIds;
            return false;
        }

        this.offendingIds = vertexIds.filter(id -> ((Long) id) <= 0);

        return this.offendingIds.count() == 0;
    }

    public DataSet<K> getOffendingIds() {
        return this.offendingIds;
    }
}
