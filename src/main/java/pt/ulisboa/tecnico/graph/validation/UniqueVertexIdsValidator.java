package pt.ulisboa.tecnico.graph.validation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.validation.GraphValidator;

public class UniqueVertexIdsValidator<K, VV, EV> extends GraphValidator<K, VV, EV> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 7294326950569738363L;

	private static TypeInformation<Integer> intTypeInfo = TypeInformation.of(Integer.class);

    private DataSet<Tuple2<K, Integer>> notUniqueIds;

    @Override
    public boolean validate(final Graph<K, VV, EV> graph) throws Exception {
        final DataSet<K> vertexIds = graph.getVertexIds();

        this.notUniqueIds = vertexIds
                .map(k -> Tuple2.of(k, 1))
                .returns(new TupleTypeInfo<>(vertexIds.getType(), UniqueVertexIdsValidator.intTypeInfo))
                .groupBy(0).sum(1)
                .filter(sum -> sum.f1 > 1);

        return this.notUniqueIds.count() == 0;
    }

    public DataSet<Tuple2<K, Integer>> getNotUniqueIds() {
        return this.notUniqueIds;
    }
}
