package pt.ulisboa.tecnico.graph.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import pt.ulisboa.tecnico.graph.algorithm.pagerank.SimplePageRank;

@SuppressWarnings("unused")
public class SMPFlinkCluster {

	public static void main(String[] args) {
    	
    	// 1000 is based on scripts/Facebook-PR.sh on line 4, penultimate argument.
    	final int outputSize = 1000;//Integer.parseInt(args[3]);
    	final int iterations = 10;//Integer.parseInt(args[2]);
    	final String dataDir = args[0];
    	final int parallelism = Integer.parseInt(args[1]);
    	
    	
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "target/GraphApprox-0.0.1-SNAPSHOT.jar");
		env.getConfig().disableSysoutLogging().setParallelism(parallelism);

        try {
        	final Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader(dataDir, env)// "/Datasets/Facebook/facebook-links-init.txt", env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges("\t")
                    .keyType(Long.class);


        	//DataSet<Tuple2<Long, Double>> result = graph.run(new SimplePageRank<>(config.getBeta(), 1.0, config.getIterations()));
        	
        	
        	//System.out.printf("Result size: {}", result.count());

        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
