package pt.ulisboa.tecnico.graph.algorithm.topdegree;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import pt.ulisboa.tecnico.graph.stream.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class TopDegreeStreamHandler extends GraphStreamHandler<Tuple2<Long, LongValue>> {

    private final EdgeDirection edgeDirection;

    private GraphUpdateTracker<Long, NullValue, NullValue> graphUpdateTracker;
    private DataSet<Tuple2<Long, LongValue>> topVertices = null;
    private String csvName = null;
    private Set<Long> updated = new HashSet<>();

    public TopDegreeStreamHandler(final EdgeDirection edgeDirection, final Map<String, Object> argValues, final Function<String, Long> f) {
        super(argValues, f, "topdegree");
        this.edgeDirection = edgeDirection;
    }

    @Override
    public void run() {
        while (true) {
            try {
            	final String update = super.pendingUpdates.take();
            	final String[] split = update.split(" ");
                switch (split[0]) {
                    case "A": {
                        // add edge
                        registerEdgeAdd(split);
                        break;
                    }
                    case "D":
                        // delete
                        registerEdgeDelete(split);
                        break;
                    case "Q":
                        super.applyUpdates();

                        final String date = split[1];
                        final double th = Double.parseDouble(split[2]);

                        this.updated = GraphUpdateTracker.allUpdatedIds(this.graphUpdateTracker.getUpdateInfos(), this.edgeDirection);
                        this.topVertices = queryTop(th);

                        this.csvName = "./top_" + date + "_" + th + ".csv";
                        this.topVertices.writeAsCsv(csvName, FileSystem.WriteMode.OVERWRITE);
                        this.topVertices.output(this.outputFormat);
                        this.env.execute();

                        this.graphUpdateTracker.reset(this.updated);
                        break;
                    case "END":
                        return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected boolean beforeUpdates(GraphUpdates<Long, NullValue> updates, GraphUpdateStatistics statistics) {
        return false;
    }

    @Override
    protected Action onQuery(final Long id, String query, Graph<Long, NullValue, NullValue> graph, GraphUpdates<Long, NullValue> updates, GraphUpdateStatistics statistics, Map<Long, GraphUpdateTracker.UpdateInfo> updateInfos) {
        return null;
    }

    @Override
    protected void onQueryResult(final Long id, String query, Action action, Graph<Long, NullValue, NullValue> graph) {

    }

    private DataSet<Tuple2<Long, LongValue>> queryTop(final Double th) throws Exception {
        if (this.topVertices == null) {
            return topDegree(this.graph, th);
        }
/*
        final DataSet<Tuple2<Long, LongValue>> updatedVertices = this.graph.inDegrees()
                .join(this.env.fromCollection(this.updated, TypeInformation.of(Long.class)))
                .where(0).equalTo(v -> v)
                .with((degree, id) -> degree)
                .returns(this.graph.inDegrees().getType());

        final Double number = this.graphUpdateTracker.getUpdateStatistics().getTotalVertices() * th + 1;

        return this.env.readCsvFile(this.csvName).types(Long.class, LongValue.class)
                .union(updatedVertices)
                .distinct(0)
                .sortPartition(1, Order.DESCENDING)
                .first(number.intValue());

        */
        return null;
    }

    private DataSet<Tuple2<Long, LongValue>> topDegree(final Graph<Long, NullValue, NullValue> graph, final double ratio) throws Exception {
        final Double number = graph.numberOfVertices() * ratio + 1;
        return graph.inDegrees().sortPartition(1, Order.DESCENDING).first(number.intValue());
    }

    @Override
    public void init() throws Exception {
        TypeInformation<Tuple2<Long, Long>> edgeTypeInfo = this.graph.getEdgeIds().getType();
        this.edgeInputFormat = new TypeSerializerInputFormat<>(edgeTypeInfo);

        this.edgeOutputFormat = new TypeSerializerOutputFormat<>();
        this.edgeOutputFormat.setInputType(edgeTypeInfo, this.env.getConfig());
        this.edgeOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        this.edgeOutputFormat.setOutputFilePath(new Path("./edges" + this.iteration));
        this.graph.getEdgeIds().output(this.edgeOutputFormat);
        this.env.execute();
    }

    @Override
    protected Long executeExact() throws Exception {
        return null;
    }

    @Override
    protected Long executeApproximate() throws Exception {
        return null;
    }

    @Override
    protected Long executeAutomatic() throws Exception {
        return null;
    }
}