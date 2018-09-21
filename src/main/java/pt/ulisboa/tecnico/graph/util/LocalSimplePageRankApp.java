package pt.ulisboa.tecnico.graph.util;

//import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

//import pt.ulisboa.tecnico.graph.algorithm.pagerank.SimplePageRank;
@SuppressWarnings("unused")
public class LocalSimplePageRankApp {

	public static void main(String[] args) {
    	final int outputSize = 1000;
    	final int iterations = 10;
    	final String dataDir = args[0];
    	
    	final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging().setParallelism(1);

        try {
        	final Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader(dataDir, env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges("\t")
                    .keyType(Long.class);

        	/*final PageRankConfig config = new PageRankConfig()
                    .setBeta(0.85)
                    .setIterations(iterations)
                    .setOutputSize(outputSize); */

        	//DataSet<Tuple2<Long, Double>> result = graph.run(new SimplePageRank<>(config.getBeta(), 1.0, config.getIterations()));
        	
        	//result.print();

        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
