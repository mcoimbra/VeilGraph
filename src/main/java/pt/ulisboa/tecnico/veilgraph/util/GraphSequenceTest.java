package pt.ulisboa.tecnico.veilgraph.util;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.RemoteEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.FilterOperator;

import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import pt.ulisboa.tecnico.veilgraph.algorithm.pagerank.PageRankParameterHelper;
import pt.ulisboa.tecnico.veilgraph.algorithm.pagerank.SimplePageRank;
import pt.ulisboa.tecnico.veilgraph.algorithm.pagerank.PageRankCsvOutputFormat;
import pt.ulisboa.tecnico.veilgraph.core.ParameterHelper;
import pt.ulisboa.tecnico.veilgraph.output.GraphOutputFormat;

@SuppressWarnings("serial")
public class GraphSequenceTest {

	private static Integer pageRankIterationCount;
	private static Integer pageRankOutputSize;
	private static Double pageRankDampeningFactor;
	private static Integer executionLimit;
	private static String streamPath;
	private static String outputBase;
	private static boolean debugging = false;
	private static String inputPath;
	
	private static final String completePageRankDirectorySuffix = "-complete";

	public static void main(final String[] args)
    {

		// Parse the options.
		final PageRankParameterHelper ph = new PageRankParameterHelper(args);
		final Map<String, Object> argValues = ph.getParameters();

		// Set some parameters of PageRank.
//		pageRankIterationCount = (Integer) argValues.get(ParameterHelper.GraphBoltArgumentName.PAGERANK_ITERATIONS.toString());
//		pageRankOutputSize = (Integer) argValues.get(ParameterHelper.GraphBoltArgumentName.PAGERANK_SIZE.toString());
//		pageRankDampeningFactor = (Double) argValues.get(ParameterHelper.GraphBoltArgumentName.DAMPENING_FACTOR.toString());
		executionLimit = (Integer) argValues.get(ParameterHelper.GraphBoltArgumentName.EXECUTION_LIMIT.toString());
		//streamPath = (String) argValues.get(ParameterHelper.GraphBoltArgumentName.STREAM_PATH.toString());
		//debugging = (Boolean) argValues.get(PageRankParameterHelper.GraphBoltArgumentName.DEBUG.toString());
		debugging = argValues.containsKey(ParameterHelper.GraphBoltArgumentName.DEBUG.toString());
		outputBase = (String) argValues.get(ParameterHelper.GraphBoltArgumentName.OUTPUT_DIR.toString());
		
		ExecutionEnvironment env = null;
		boolean runningRemoteFlink = 
				argValues.containsKey(ParameterHelper.GraphBoltArgumentName.SERVER_ADDRESS.toString()) &&
				argValues.containsKey(ParameterHelper.GraphBoltArgumentName.SERVER_PORT.toString());
		if(runningRemoteFlink) {

			final String remoteAddress = (String) argValues.get(ParameterHelper.GraphBoltArgumentName.SERVER_ADDRESS.toString());
			final Integer remotePort = (Integer) argValues.get(ParameterHelper.GraphBoltArgumentName.SERVER_PORT.toString());

			String jarFile = "";
			File mvnRootFile = null;
			try {
				// Get root of the project in the file system and escape HTML-based URL characters.
				mvnRootFile = new java.io.File(GraphSequenceTest.class.getProtectionDomain()
						.getCodeSource()
						.getLocation()
						.getPath()
						.replaceAll("%20", " "))
						.getParentFile().getParentFile();

				// Get the full path to the pom.xml.
				final File[] pomFile = mvnRootFile.listFiles(new FilenameFilter() {
					public boolean accept(File dir, String filename) {
						return filename.equals("pom.xml");
					}
				});


				// Read the pom.xml.
				final Scanner scanner = new Scanner(pomFile[0]);
				String artifactId = null;
				String version = null;
				String packaging = ".jar";

				// Find out the name of the .jar file to send to the remote Flink cluster.
				while (scanner.hasNextLine()) {
					String line = scanner.nextLine().trim();
					if (line.startsWith("<artifactId>")) {
						artifactId = line.substring(line.indexOf(">") + 1, line.lastIndexOf("<"));
					}
					if (line.startsWith("<version>")) {
						version = line.substring(line.indexOf(">") + 1, line.lastIndexOf("<"));
					}
					if (artifactId != null && version != null) {
						jarFile = artifactId + "-" + version + packaging;
						break;
					}
				}
				scanner.close();

			} catch (FileNotFoundException e) {
				e.printStackTrace();
				System.exit(1);
			}
			
			jarFile = mvnRootFile.toString() + java.nio.file.FileSystems.getDefault().getSeparator() + "target" + java.nio.file.FileSystems.getDefault().getSeparator() + jarFile;
			
			System.out.println(String.format("Starting remote Flink execution environment: %s:%d", remoteAddress, remotePort));
			//System.out.println(mvnRootFile.toString());
			System.out.println(String.format("Jar file: %s", jarFile));
			
			//System.exit(0);
			
			// http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/flink-akka-OversizedPayloadException-error-td12218.html
			//env = ExecutionEnvironment.createRemoteEnvironment(remoteAddress, remotePort, jarFile);
			String[] jarFiles = new String[1];
			jarFiles[0] = jarFile;
			
			
			// https://ci.apache.org/projects/flink/flink-docs-release-1.3/api/java/org/apache/flink/configuration/ConfigConstants.html#AKKA_FRAMESIZE
			Configuration clientConfig = new Configuration();
			
			final String akkaFrameSize = "256000kB";
			
			//ConfigOption<String> akkaConfig = new ConfigOption<String>(AkkaOptions.FRAMESIZE.key(), akkaFrameSize);
			
			ConfigOption<String> akkaConfig = ConfigOptions.key(AkkaOptions.FRAMESIZE.key()).defaultValue(akkaFrameSize);
			
			clientConfig.setString(akkaConfig, akkaFrameSize);
			
			//clientConfig.setString(akkaConfig);
			
			
			
			
			env = new RemoteEnvironment(remoteAddress, remotePort, clientConfig, jarFiles, null);
			
		}
		else {
			System.out.println(String.format("Starting local Flink execution environment"));
			Configuration conf = new Configuration();
			
			//conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
			//conf.setString(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, value);
			
			env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		}

		// Some details on the ExecutionEnvironment configuration options: https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/index.html#debugging
		env.getConfig().enableSysoutLogging().setParallelism(1);
		// TODO: experiment with this later: .enableObjectReuse(); //https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/index.html#object-reuse-enabled
		
		// Read the initial graph.
		inputPath = (String) argValues.get(ParameterHelper.GraphBoltArgumentName.INPUT_FILE.toString());
    	Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader(inputPath, env)
                .ignoreCommentsEdges("#").fieldDelimiterEdges("\t").keyType(Long.class);

    	try {
    		//env.startNewSession();


    		/*System.out.println(String.format("Initial graph has %d vertices and %d edges.", 
					graph.numberOfVertices(),
					graph.numberOfEdges()));
*/

		} catch (Exception e1) {
			e1.printStackTrace();
		}

    	System.out.println("Finished parsing arguments.");
    	
		final int updateSize = 100;
    	optimizedAccumulationLoop(updateSize, graph, env);
    }
	
	private static void optimizedAccumulationLoop(final int updateSize, Graph<Long, NullValue, NullValue> graph, final ExecutionEnvironment env) {
		try (final BufferedReader br = new BufferedReader(new FileReader(streamPath))) {
			
			final ArrayList<String> updates = new ArrayList<String>();
			final ArrayList<String> plans = new ArrayList<String>();
			final HashMap<Long, String> vids = new HashMap<>();
			final HashMap<Long, String> eids = new HashMap<>();
			
			// Retrieve input file name without the file extension.
	        final String datasetName = inputPath.contains(java.nio.file.FileSystems.getDefault().getSeparator()) ? 
	        		inputPath.substring(inputPath.lastIndexOf(java.nio.file.FileSystems.getDefault().getSeparator()) + 1, inputPath.lastIndexOf(".")) :
	        			inputPath.substring(inputPath.lastIndexOf("/") + 1, inputPath.lastIndexOf("."));
	        final String datasetOutputToken = String.format("%s_%d_%d_%02.2f", datasetName, pageRankIterationCount, pageRankOutputSize, pageRankDampeningFactor);
			
	        final String resultsDir = new StringJoiner(java.nio.file.FileSystems.getDefault().getSeparator())
					.add(outputBase)
	    			.add("Results")
	    			.add("PR")
	    			.add(datasetOutputToken + completePageRankDirectorySuffix)
	    			.toString();
	    	
	    	System.out.println(String.format("Results: %s", resultsDir));
	    	// Create output directory if it does not exist.
            new File(resultsDir).mkdirs();
            
			Long executionCounter = 0L;
			DataSet<Tuple2<Long, Double>> previousRanks = null;
			String line;
			while ((line = br.readLine()) != null) {
				
				updates.add(line.trim());
				
				if(updates.size() % updateSize == 0) {
					executionCounter++;
					
					// Convert list of String to Flink Gelly Edge.
					final List<Edge<Long, NullValue>> edgesToAdd = new ArrayList<Edge<Long, NullValue>>();
					final List<Vertex<Long, NullValue>> verticesToAdd = new ArrayList<Vertex<Long, NullValue>>();
					for(String s : updates) {
						parseVertices(s, verticesToAdd);
						edgesToAdd.add(parseEdge(s));
					}

					// Add to internal Gelly Graph DataSets.
					// TODO: test this by reassigning the graph variable: graph = graph.addVertices(verticesToAdd).addEdges(edgesToAdd);
					
					/*
					final Graph<Long, NullValue, NullValue> newGraph = graph
							.addVertices(verticesToAdd);
							.addEdges(edgesToAdd);
						*/
					
							
					final DataSet<Vertex<Long, NullValue>> newVertices = graph.getVertices().coGroup(env.fromCollection(verticesToAdd))
							.where(0).equalTo(0).with(new VerticesUnionCoGroup<>()).name("Add vertices");
					
					final DataSet<Edge<Long, NullValue>> newEdgesDataSet = env.fromCollection(edgesToAdd);

					final DataSet<Edge<Long, NullValue>> validNewEdges = newVertices
							.join(newEdgesDataSet, JoinHint.REPARTITION_HASH_FIRST)
							.where(0).equalTo(0)
							.with(new JoinVerticesWithEdgesOnSrc<Long, NullValue, NullValue>()).name("Join with source")
							.join(newVertices, JoinHint.REPARTITION_HASH_SECOND).where(1).equalTo(0)
							.with(new JoinWithVerticesOnTrg<Long, NullValue, NullValue>()).name("Join with target");

					final Graph<Long, NullValue, NullValue> newGraph = Graph.fromDataSet(newVertices, graph.getEdges().union(validNewEdges), env);
							
							
					
					// Generate identifications for the vertex counter.
					final String vid = new AbstractID().toString();
					newGraph.getVertices().output(new Utils.CountHelper<Vertex<Long, NullValue>>(vid)).name("count()");
					vids.put(executionCounter, vid);

					// Generate identifications for the edge counter.
					final String eid = new AbstractID().toString();
					newGraph.getEdges().output(new Utils.CountHelper<Edge<Long, NullValue>>(eid)).name("count()");
					eids.put(executionCounter, eid);
					
					DataSet<Tuple2<Long, Double>> result = null;
					
					if(previousRanks != null)
						result = newGraph.run(new SimplePageRank<>(pageRankDampeningFactor, previousRanks, pageRankIterationCount));
					else
						result = newGraph.run(new SimplePageRank<>(pageRankDampeningFactor, 1.0, pageRankIterationCount));
					
					// Output the current ranks to a file.
					final GraphOutputFormat<Tuple2<Long, Double>> outputFormat = new PageRankCsvOutputFormat(resultsDir, System.lineSeparator(), ";", false, true);
		    		outputFormat.setName("exact_PR");
					outputFormat.setIteration(executionCounter);
			        outputFormat.setTags("");
                    result.sortPartition(1, Order.DESCENDING).setParallelism(1).first(pageRankOutputSize).output(outputFormat).name("Store ranks");
					previousRanks = result;
				}

				// For debugging purposes, stop execution without consuming all updates if desired.
				if (executionLimit != -1 && executionCounter.intValue() == executionLimit) {
					break;
				}
			}

			System.out.println("Finished building the job plan operators.");
			
			/*
			final String rankResultID = new AbstractID().toString();
			previousRanks.output(new Utils.CountHelper<Tuple2<Long, Double>>(rankResultID)).name("count()");
			 */
			
			// Store the current execution plan as a JSON.
			/*final String p = env.getExecutionPlan();
			plans.add(p);

			*/
			
			System.out.println("Triggering job execution.");
			long start = System.nanoTime();

			final JobExecutionResult res = env.execute("GraphBolt execution");

			long end = System.nanoTime();

			/*
			final long rankCount = res.<Long> getAccumulatorResult(rankResultID);
			System.out.println(String.format("%d-th PageRank has %d elements. (%d.%d s).",
					executionCounter,
					rankCount,
					res.getNetRuntime(TimeUnit.SECONDS),
					res.getNetRuntime(TimeUnit.MILLISECONDS) % 1000)); */

			// Prepare address of Flink's REST endpoint access.
			final String restIPV4Address = "127.0.0.1";
			final String restIPV4Port = "8081";
			final String restEndpoint = String.format("http://%s:%s/jobs/%s/vertices", restIPV4Address, restIPV4Port, res.getJobID().toString());

			System.out.println("Fetching the Apache Flink job statistics.");
			final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			final HttpGet httpget = new HttpGet(restEndpoint);
			final HttpResponse response = httpClient.execute(httpget);
			final HttpEntity entity = response.getEntity();
			final InputStream restResult = entity.getContent();
			final String restResultString = new BufferedReader(new InputStreamReader(restResult)).lines().collect(Collectors.joining("\n"));
			final JsonParser parser = new JsonParser();
			final JsonElement element = parser.parse(restResultString);

			// Parse the JSON and retrieve the execution durations.
			System.out.println("Parsing...");
			final HashMap<Integer, Integer> graphUpdateTime = new HashMap<Integer, Integer>();
			final HashMap<Integer, String> pageRankTime = new HashMap<Integer, String>();
			if (element.isJsonObject()) {
				final JsonObject jobDetails = element.getAsJsonObject();
				final JsonArray flinkDataflowNodes = jobDetails.getAsJsonArray("vertices");
				int ctr = 1;
				for (int i = 0; i < flinkDataflowNodes.size(); i++) {
					final JsonObject flinkOperator = flinkDataflowNodes.get(i).getAsJsonObject();
					final String operatorName = flinkOperator.get("name").getAsString();
					
					if(operatorName.startsWith("IterationHead(Simple PageRank")) {
						System.out.println(operatorName.toString());
						
						pageRankTime.put(ctr, flinkOperator.get("duration").getAsString());
						ctr++;
					}
					else if (operatorName.startsWith("CoGroup (Add vertices)")) {
						System.out.println(operatorName.toString());
						if (! graphUpdateTime.containsKey(ctr))
							graphUpdateTime.put(ctr, flinkOperator.get("duration").getAsInt());
						else
							graphUpdateTime.put(ctr, graphUpdateTime.get(ctr) + flinkOperator.get("duration").getAsInt());
					}
					else if (operatorName.startsWith("Join (Join with source)")) {
						System.out.println(operatorName.toString());
						if (! graphUpdateTime.containsKey(ctr))
							graphUpdateTime.put(ctr, flinkOperator.get("duration").getAsInt());
						else
							graphUpdateTime.put(ctr, graphUpdateTime.get(ctr) + flinkOperator.get("duration").getAsInt());
					}
					else if (operatorName.startsWith("Join (Join with target)")) {
						System.out.println(operatorName.toString());
						if (! graphUpdateTime.containsKey(ctr))
							graphUpdateTime.put(ctr, flinkOperator.get("duration").getAsInt());
						else
							graphUpdateTime.put(ctr, graphUpdateTime.get(ctr) + flinkOperator.get("duration").getAsInt());
					}
				}
			}
				
			// Print execution statistics.
			// https://ci.apache.org/projects/flink/flink-docs-release-1.4/monitoring/rest_api.html#details-of-a-running-or-completed-job
			// https://stackoverflow.com/questions/33691612/apache-flink-stepwise-execution/33691957#33691957
			final List<Long> sortedList = new ArrayList<Long>(eids.keySet());
			Collections.sort(sortedList);
			final StringBuilder statsBuilder = new StringBuilder("iteration;nVertices;nEdges;graphUpdateTime;pageRankTime;iterations\n");
			for(Long k : sortedList) {
				final String currentLine = String.format("%d;%d;%d;%s;%s;%d\n", k, res.<Long> getAccumulatorResult(vids.get(k)), res.<Long> getAccumulatorResult(eids.get(k)), graphUpdateTime.get(k).toString(), pageRankTime.get(k), pageRankIterationCount);
				statsBuilder.append(currentLine);
			}

			
			final String statisticsDir = new StringJoiner(java.nio.file.FileSystems.getDefault().getSeparator())
    				.add(outputBase)
        			.add("Statistics")
        			.add("PR")
        			.add(datasetOutputToken + completePageRankDirectorySuffix)
        			.toString();
    		System.out.println(String.format("Statistics: %s", statisticsDir));
    		// Create output directory if it does not exist.
            new File(statisticsDir).mkdirs();
			final String statsFileName = String.format(statisticsDir + java.nio.file.FileSystems.getDefault().getSeparator() + datasetName + ".tsv");
			final BufferedWriter statsOut = new BufferedWriter(new FileWriter(statsFileName));
			statsOut.write(statsBuilder.toString());
			statsOut.close();


			System.out.println(String.format("Total job execution time in Java: (%d.%d s).",
					TimeUnit.NANOSECONDS.toSeconds(end - start),
					TimeUnit.NANOSECONDS.toMillis(end - start) % 1000));


			// Write the generated execution plans.
			if (debugging) {
				final String debugDir = new StringJoiner(java.nio.file.FileSystems.getDefault().getSeparator())
	    				.add(outputBase)
	        			.add("Debug")
	        			.add("PR")
	        			.add(datasetOutputToken + completePageRankDirectorySuffix)
	        			.toString();
	    		System.out.println(String.format("Debug: %s", debugDir));
	    		// Create output directory if it does not exist.
	            new File(debugDir).mkdirs();
				
	            final String jobResultFileName = String.format(debugDir + java.nio.file.FileSystems.getDefault().getSeparator() + datasetName + "-job-stats.json");
				final BufferedWriter jsonOut = new BufferedWriter(new FileWriter(jobResultFileName));
				jsonOut.write(restResultString + "\n");
				jsonOut.close();
			
				/*
				int i = 0;
				for (String plan : plans) {
					final String planFileName = String.format(debugDir + java.nio.file.FileSystems.getDefault().getSeparator() + datasetName + "-plan.json");
					final BufferedWriter out = new BufferedWriter(new FileWriter(planFileName));
					out.write(plan + "\n");
					out.close();
				}
				*/
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
	
	@ForwardedFieldsSecond("f0; f1; f2")
	private static final class JoinVerticesWithEdgesOnSrc<K, VV, EV> implements
			JoinFunction<Vertex<K, VV>, Edge<K, EV>, Edge<K, EV>> {

		@Override
		public Edge<K, EV> join(Vertex<K, VV> vertex, Edge<K, EV> edge) throws Exception {
			return edge;
		}
	}

	@ForwardedFieldsFirst("f0; f1; f2")
	private static final class JoinWithVerticesOnTrg<K, VV, EV> implements
			JoinFunction<Edge<K, EV>, Vertex<K, VV>, Edge<K, EV>> {

		@Override
		public Edge<K, EV> join(Edge<K, EV> edge, Vertex<K, VV> vertex) throws Exception {
			return edge;
		}
	}
	
	private static class EdgeUpdateFlagger implements MapFunction<Tuple2<Long, Long>, Edge<Long, Long>> {
		long ctr = 0L;
		private final long updateSize;
		
		public EdgeUpdateFlagger(long updateSize) {
			this.updateSize = updateSize;
		}
		
		@Override
		public Edge<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
			return new Edge<Long, Long>(value.f0, value.f1, (ctr++ < updateSize ? 1L : 0L) );
		}
	}
	
	private static class EdgeUpdateStarter implements CoGroupFunction<Edge<Long, Long>, Edge<Long, Long>, Edge<Long, Long>> {
		
		// Note: this does not consider the existence of duplicate edges.
		@Override
		public void coGroup(Iterable<Edge<Long, Long>> first, Iterable<Edge<Long, Long>> second,
				Collector<Edge<Long, Long>> out) throws Exception {
			Iterator<Edge<Long, Long>> iter = first.iterator();
			
			if(iter.hasNext()) {
				Edge<Long, Long> e = iter.next();
				out.collect(e);
			}
			else {
				Iterator<Edge<Long, Long>> unchangedIterator = second.iterator();
				out.collect(unchangedIterator.next());
			}
		}
	}

	// Flink iteration that doesn't work. (DOES NOT WRITE TO SINKS)
	private static void iterativeFlinkLoop(final String streamPath, final int updateSize, final int executionLimit, Graph<Long, NullValue, NullValue> graph, final ExecutionEnvironment env) {
		
		final DataSet<Edge<Long, Long>> updateDataSet = env
			.readCsvFile(streamPath)
			.fieldDelimiter("\t")
			.ignoreComments("#")
			.types(Long.class, Long.class)
			.map(new EdgeUpdateFlagger(updateSize))
			.setParallelism(1);
		
		
		IterativeDataSet<Edge<Long, Long>> initial = updateDataSet
				.iterate(executionLimit);
		
		// Get the edges which are currently going to be used as an update.
		FilterOperator<Edge<Long, Long>> updateFilter = initial.filter(new FilterFunction<Edge<Long, Long>>() {
			@Override
			public boolean filter(Edge<Long, Long> value) throws Exception {
				return value.f2 == 1L;
			}
		}).name("Current update edges");
			
		// Generate the vertex DataSet containing the initial vertices plus added.
		DataSet<Vertex<Long, NullValue>> vertexUpdateDataSet = updateFilter
			.flatMap(new FlatMapFunction<Edge<Long, Long>, Vertex<Long, NullValue>>() {
				@Override
				public void flatMap(Edge<Long, Long> value, Collector<Vertex<Long, NullValue>> out)
						throws Exception {
					out.collect(new Vertex<Long, NullValue>(value.f0, NullValue.getInstance()));
					out.collect(new Vertex<Long, NullValue>(value.f1, NullValue.getInstance()));
				}
			})
			.distinct()
			// put them together with the graph vertices
			.coGroup(graph.getVertices())
			.where(0).equalTo(0)
			.with(new VerticesUnionCoGroup<>())
			.name("Add vertices");
		
		// Generate the edge DataSet containing the initial edges plus added.
		DataSet<Edge<Long, NullValue>> edgeUpdateDataSet = updateFilter
			.map(new MapFunction<Edge<Long, Long>, Edge<Long, NullValue>>() {
				@Override
				public Edge<Long, NullValue> map(Edge<Long, Long> value) throws Exception {
					return new Edge<Long, NullValue>(value.f0, value.f1, NullValue.getInstance());
				}
			})
			.union(graph.getEdges())
			.name("Add edges");
				
		
		/*
		Graph<Long, NullValue, NullValue> newGraph = Graph.fromDataSet(vertexUpdateDataSet, edgeUpdateDataSet, env);
		
		try {
			//System.out.println(newGraph.numberOfEdges());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		// Get the edges which will be delayed to the next Flink iteration.
		FilterOperator<Edge<Long, Long>> delayFilter = initial
			.filter(new FilterFunction<Edge<Long, Long>>() {
				@Override
				public boolean filter(Edge<Long, Long> value) throws Exception {
					return value.f2 == 0L;
				}
			}).name("Delayed edges");
		
		// Flag the portion of delayed edges to be used in the next Flink iteration.
		DataSet<Edge<Long, Long>> iteration = delayFilter
			.first(updateSize)
			.coGroup(delayFilter)
			.where(0, 1)
			.equalTo(0, 1)
			.with(new EdgeUpdateStarter())
			.union(updateFilter)
			.name("Flag edges");
		
		// Feed the target updates to the next Flink iteration.
		DataSet<Edge<Long, Long>> result = initial.closeWith(iteration);
		
		// Store the current execution plan as a JSON.
		try {
			
			final String id = new AbstractID().toString();
			result.output(new Utils.CountHelper<Edge<Long, Long>>(id)).name("count()");
			
			final String p = env.getExecutionPlan();
			final JobExecutionResult res = env.execute();
			
			final long resultCount = res.<Long> getAccumulatorResult(id);
			System.out.println(String.format("Iterative program yielded %d edges", resultCount));
			
			final String planFileName = String.format("iterative-plan.json");
			final BufferedWriter out = new BufferedWriter(new FileWriter(planFileName));
			out.write(p + "\n");
			out.close();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	// Java iteration with accumulated operator execution (BAD PERFORMANCE)
	private static void unoptimizedLoop(final String streamPath, final int updateSize, final int executionLimit, Graph<Long, NullValue, NullValue> graph, final ExecutionEnvironment env) {
		try (final BufferedReader br = new BufferedReader(new FileReader(streamPath))) {
			
			final ArrayList<String> updates = new ArrayList<String>();
			final ArrayList<String> plans = new ArrayList<String>();
			String line;
			
			UnionOperator<Edge<Long, NullValue>> edgeOp = null;
			CoGroupOperator<Vertex<Long, NullValue>, Vertex<Long, NullValue>, Vertex<Long, NullValue>> vertexOp = null;
			
			int executionCounter = 0;
			while ((line = br.readLine()) != null) {
				
				updates.add(line.trim());
				
				if(updates.size() == updateSize) {
					executionCounter++;
					
					// Convert list of String to Gelly Edge.
					final List<Edge<Long, NullValue>> edgesToAdd = new ArrayList<Edge<Long, NullValue>>();
					final List<Vertex<Long, NullValue>> verticesToAdd = new ArrayList<Vertex<Long, NullValue>>();
					for(String s : updates) {
						parseVertices(s, verticesToAdd);
						edgesToAdd.add(parseEdge(s));
					}

					// Add to internal Gelly Graph DataSets.
					System.out.println(String.format("Added %d vertices and %d edges.", verticesToAdd.size(), edgesToAdd.size()));
					
					final DataSet<Edge<Long, NullValue>> newEdgesDataSet = env.fromCollection(edgesToAdd);
					
					// edgeOp accumulates UnionOperator instances containing new edge updates. The time used to compute edge updates thus increases with the number of accumulated updates.
					if(edgeOp == null) {
						edgeOp = graph.getEdges().union(newEdgesDataSet); // case of the first update.
					}
					else {
						edgeOp = edgeOp.union(newEdgesDataSet);
					}
					
					// vertexOp accumulates CoGroupFunction instances containing new vertex updates. The time used to compute vertex updates thus increases with the number of accumulated updates.
					if(vertexOp == null) {
						vertexOp = graph // case of the first update.
								.getVertices()
								.coGroup(env.fromCollection(verticesToAdd))
								.where(0).equalTo(0)
								.with(new VerticesUnionCoGroup<>()).name("Add vertices");
					}
					else {
						vertexOp = vertexOp
							.coGroup(env.fromCollection(verticesToAdd))
							.where(0).equalTo(0)
							.with(new VerticesUnionCoGroup<>()).name("Add vertices");
					}
					
					Graph<Long, NullValue, NullValue> newGraph = 
							Graph.fromDataSet(vertexOp, edgeOp, env);
					
					// Generate identifications for the vertex and edge counters respectively.
					final String vid = new AbstractID().toString();
					newGraph.getVertices().output(new Utils.CountHelper<Vertex<Long, NullValue>>(vid)).name("count()");
					
					final String eid = new AbstractID().toString();
					newGraph.getEdges().output(new Utils.CountHelper<Edge<Long, NullValue>>(eid)).name("count()");
					
					// Store the current execution plan as a JSON.
					final String p = env.getExecutionPlan();
					plans.add(p);
					
					// Trigger the Flink job and collect the counts.
					final JobExecutionResult res = env.execute();
					final long vertexCount = res.<Long> getAccumulatorResult(vid);
					final long edgeCount = res.<Long> getAccumulatorResult(eid);

					
					System.out.println(String.format("%d-th graph now has %d vertices and %d edges (%d.%d s).", 
							executionCounter,
							vertexCount,
							edgeCount,
							env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS),
							env.getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS) % 1000));
					
					//String plan = lenv.getExecutionPlan();
					//System.out.println(plan);
					
					/*
					// Execute PageRank. This will trigger an internal Flink job.
					final DataSet<Tuple2<Long, Double>> ranks =
		    				graph.run(new SimplePageRank<>(beta, previousRanks, iterations));
					
					
					System.out.println(String.format("Calculated %d ranks (%d.%d s).", 
							ranks.count(),
							env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS),
							env.getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS) % 1000));
					
					previousRanks = ranks;
					*/
					updates.clear();
				}
				
				if (executionLimit != -1 && executionCounter == executionLimit) {
					int i = 0;
					for(String plan : plans) {
						final String planFileName = String.format("plan-%d.json", ++i);
						final BufferedWriter out = new BufferedWriter(new FileWriter(planFileName));
						out.write(plan + "\n");
						out.close();
					}
					
					break;
				}
			}
			
			Scanner reader = new Scanner(System.in);
			reader.next().charAt(0);

			reader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
	
	
	private static void parseVertices(String line, List<Vertex<Long, NullValue>> verticesToAdd) {
		final String data[] = line.split("\\s+");
		
		Vertex<Long, NullValue> src = new Vertex<Long, NullValue>(Long.valueOf(data[0]), NullValue.getInstance());
		Vertex<Long, NullValue> dst = new Vertex<Long, NullValue>(Long.valueOf(data[1]), NullValue.getInstance());
		
		if(! verticesToAdd.contains(src))
			verticesToAdd.add(src);
		
		if(! verticesToAdd.contains(dst))
			verticesToAdd.add(dst);
	}

	private static Edge<Long, NullValue> parseEdge(final String line) {
		final String data[] = line.split("\\s+");
		return new Edge<Long, NullValue>(Long.valueOf(data[0]), Long.valueOf(data[1]), NullValue.getInstance());
	}
	
	
	public static final class VerticesUnionCoGroup<K, VV> implements CoGroupFunction<Vertex<K, VV>, Vertex<K, VV>, Vertex<K, VV>> {

		@Override
		public void coGroup(Iterable<Vertex<K, VV>> oldVertices, Iterable<Vertex<K, VV>> newVertices,
							Collector<Vertex<K, VV>> out) throws Exception {

			final Iterator<Vertex<K, VV>> oldVerticesIterator = oldVertices.iterator();
			final Iterator<Vertex<K, VV>> newVerticesIterator = newVertices.iterator();

			// if there is both an old vertex and a new vertex then only the old vertex is emitted
			if (oldVerticesIterator.hasNext()) {
				out.collect(oldVerticesIterator.next());
			} else {
				out.collect(newVerticesIterator.next());
			}
		}
	}
	
}
