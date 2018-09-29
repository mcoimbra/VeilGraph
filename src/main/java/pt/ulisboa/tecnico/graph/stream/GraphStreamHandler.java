package pt.ulisboa.tecnico.graph.stream;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.*;
import pt.ulisboa.tecnico.graph.core.ParameterHelper;
import pt.ulisboa.tecnico.graph.model.GraphModel;
import pt.ulisboa.tecnico.graph.output.DiscardingGraphOutputFormat;
import pt.ulisboa.tecnico.graph.output.GraphOutputFormat;
import pt.ulisboa.tecnico.graph.util.GraphUtils;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Abstract class to handle updates to a graph coming as a stream.
 *
 * @param <R>
 * @author Miguel E. Coimbra
 */
public abstract class GraphStreamHandler<R> implements Runnable {


    private static GraphStreamHandler singleton;

    /**
     * Are we saving Flink jobs' information from the Web Manager as JSON files?
     */
    private final boolean saveFlinkJobOperatorJSON;
    private String flinkJobManagerAddress;
    private boolean isRunningRemote;
    private String flinkJobManagerPort;
    private boolean runningLocalFlinkWebUI;

    /**
     * Check if we are saving Flink job information as JSON.
     * See {@link GraphStreamHandler#saveFlinkJobOperatorJSON}
     */
    public boolean savingFlinkJobOperatorJSON() {
        return this.saveFlinkJobOperatorJSON;
    }

    /**
     * Is the Flink cluster running in remote mode?
     */
    public boolean isRunningRemote() {
        return this.isRunningRemote;
    }

    /**
     * Get the stored Flink JobManager address.
     * @return The JobManager address.
     */
    public String getFlinkJobManagerAddress() {
        return this.flinkJobManagerAddress;
    }

    public static GraphStreamHandler getInstance() {
        return GraphStreamHandler.singleton;
    }

    /**
     * Directory name to reflect the algorithm running on GraphBolt. (e.g. "pagerank").
     */
    protected String algorithmName;
    /**
     * Is GraphBolt running in debug mode?
     */
    protected final Boolean debugging;
    /**
     * Prefix of the output file names, based on the input file. (e.g. '-i file.tsv' results in "file").
     */
    protected final String datasetName;
    /**
     * Parameters provided by the user.
     */
    protected final Map<String, Object> argValues;
    // Statistics representing time are always stored in milliseconds.
    protected final HashMap<String, ArrayList<Long>> statisticsMap = new HashMap<>();
    protected final ArrayList<String> statOrder = new ArrayList<String>();
    /**
     * Should GraphBolt delete the directories and files created in the temporary directory?
     * {@link GraphStreamHandler#tempDirectory}
     */
    private final boolean keepingTemporaryDirectory;
    /**
     * Directory where GraphBolt will create all subdirectories ("Results", "Statistics", etc.).
     */
    private final String rootDirectory;
    /**
     * Path to file containing the initial graph.
     */
    private final String inputPath;
    /**
     * Do we want to tell the graph summary model to write relevant details to disk?
     */
    private final boolean dumpingModel;
    /**
     * Should GraphBolt keep intermediate graph files and algorithm results?
     * See: {@link GraphStreamHandler#cacheDirectory}
     */
    private final boolean keepingCacheDirectory;
    /**
     * Should GraphBolt keep Apache Flink's logs intermediate logs?
     * See: {@link GraphStreamHandler#loggingDirectory}
     */
    private final boolean keepingLogDirectory;
    /**
     * Should Flink execution plans be written to disk?
     * See: {@link GraphStreamHandler#plansDirectory}
     */
    private final boolean saveFlinkPlans;
    /**
     * Are we saving Flink job operator statistics?
     */
    public boolean savingFlinkPlans() {
        return this.saveFlinkPlans;
    }
    /**
     * Get the current statistics directory.
     * See: {@link GraphStreamHandler#statisticsDirectory}
     */
    public String getStatisticsDirectory() {
        return this.statisticsDirectory;
    }
    /**
     * Should Flink job execution operator statistics be written to disk?
     * See: {@link GraphStreamHandler#statisticsDirectory}
     */
    private final boolean saveFlinkJobOperatorStatistics;
    /**
     * Are we saving Flink job operator statistics?
     */
    public boolean savingFlinkJobOperatorStatistics() {
        return this.saveFlinkJobOperatorStatistics;
    }
    /**
     * Vertex extractor lambda for the string-based stream of graph updates.
     */
    final private Function<String, Long> vertexIdTypeParser;
    /**
     * Stored updates received from the stream which haven't been looked at yet.
     */
    protected BlockingQueue<String> pendingUpdates;
    /**
     * Apache Flink execution environment.
     */
    protected ExecutionEnvironment env;
    /**
     * The model of summarized graph in use.
     */
    protected GraphModel model = null;
    /**
     * Current Apache Flink Gelly graph.
     */
    protected Graph<Long, NullValue, NullValue> graph;
    protected GraphOutputFormat<R> outputFormat;
    /**
     * Stored updates with associated vertex degree changes and a register of additions/deletions of vertices/edges.
     */
    protected GraphUpdateTracker<Long, NullValue, NullValue> graphUpdateTracker;
    /**
     * Format to read the graph from secondary storage between executions.
     */
    protected TypeSerializerInputFormat<Tuple2<Long, Long>> edgeInputFormat;
    /**
     * Format to write the graph to secondary storage between executions.
     */
    protected TypeSerializerOutputFormat<Tuple2<Long, Long>> edgeOutputFormat;
    /**
     * Custom output file suffix name, based on the employed graph model. (e.g. "0.05_1_0.02" could be generated from the BigVertexGraph model with updateRatio = 0.05, neighborhood = 1 and delta = 0.02)
     */
    protected String customName;
    /**
     * We start counting executions from 0.
     * Each execution is a point in time (since GraphBolt started running) where the user executed a query over the graph (doesn't matter if it is an exact, approximate, automatic or any other type of execution strategy).
     */
    protected Long iteration = 0L;
    /**
     * Directory to store statistics of the executed graph algorithm plus overheads incurred from using a given graph model.
     */
    protected String statisticsDirectory;
    /**
     * Directory to store intermediate and auxiliary graph outputs.
     */
    protected String cacheDirectory = null;
    /**
     * Directory for Flink's temporary files. Defaults to the system's temporary directory.
     */
    protected String tempDirectory = System.getProperty("java.io.tmpdir");
    /**
     * Currently we are storing on disk the resulting graphs of the last two executions.
     */
    protected int storedIterations = 2;
    /**
     * Directory for models (if used) to output their representation to disk.
     */
    protected String modelDirectory = null;
    /**
     * Directory to store results of the executed graph algorithm.
     */
    protected String resultsDirectory = null;
    // Current execution strategy - starting default is exact computation.
    protected Action executionStrategy;
    /**
     * How many executions should GraphBolt wait to write the graph model to disk?
     */
    private Integer snapshotFrequency = 1;
    /**
     * Execution counter. Triggers graph model disk dump every {@link GraphStreamHandler#snapshotFrequency}
     */
    private Integer snapshotCtr = 0;
    /**
     * Directory to store Apache Flink logging outputs.
     */
    private String loggingDirectory = null;
    private Integer streamPort = -1;
    /**
     * Directory where Flink execution plans will be stored.
     */
    private String plansDirectory;
    private StreamProvider<String> updateStream;
    /**
     * Statistics are written to this file located in {@link GraphStreamHandler#statisticsDirectory}
     */
    private transient PrintStream statisticsPrintStream;


    /**
     * Should we periodically store the total of all algorithm results?
     */
    protected boolean checkingPeriodicFullAccuracy;


    private short ELEMENT_COUNT_PER_UPDATE_MSG = 3;
    private short UPDATE_SOURCE_VERTEX_INDEX = 1;
    private short UPDATE_TARGET_VERTEX_INDEX = 2;


    /**
     * Prepare Flink cluster configuration and create one in either local or remote mode.
     * Configure GraphBolt output directories and files.
     *
     * @param argValues The user-provided program arguments.
     * @param f A vertex-parsing function.
     * @param graphAlgorithmName The name to identify the graph algorithm being processed by GraphBolt. Used to create directory names.
     */
    public GraphStreamHandler(final Map<String, Object> argValues, final Function<String, Long> f, String graphAlgorithmName) {

        this.argValues = argValues;
        this.vertexIdTypeParser = f;

        this.outputFormat = new DiscardingGraphOutputFormat<>();

        this.algorithmName = graphAlgorithmName;

        // Default execution strategy is to recompute everything on every update.
        this.executionStrategy = Action.COMPUTE_EXACT;


        //TODO: quando se faz get a este campo com os enums (ex: PageRankParameterHelper), é preciso chamar ".toString" sobre os enums. Era bom encapsular este argValues numa estrutura com um .get() que recebesse um objeto (seriam valores dos enum PageRankParameterHelper e GraphBoltParameterHelper) e que internamente chamasse o .toString() desse objeto ()

        //TODO: talvez esse mapa pudesse herdar de Map e os seus elementos pudessem receber parametrizações de tipos. Assim evitava-se o cast (Double),(Integer) e afins cada vez que se quer aceder a um parâmetro do argValues

        // Em vez de: argValues.get(PageRankParameterHelper.PageRankArgumentName.DAMPENING_FACTOR.toString())
        // Seria: gbMaparg.get(PageRankParameterHelper.PageRankArgumentName.DAMPENING_FACTOR)

        // Directory where GraphBolt will create directories for statistics, results, etc.
        this.rootDirectory = (String) argValues.get(ParameterHelper.GraphBoltArgumentName.OUTPUT_DIR.toString());


        this.debugging = argValues.containsKey(ParameterHelper.GraphBoltArgumentName.DEBUG.toString());

        // Initial graph file path.
        this.inputPath = (String) argValues.get(ParameterHelper.GraphBoltArgumentName.INPUT_FILE.toString());

        // Retrieve input file name without the file extension.
        this.datasetName = this.inputPath.substring(this.inputPath.lastIndexOf("/") + 1, this.inputPath.lastIndexOf("."));

        // Did the user provide a temporary directory location?
        if(argValues.containsKey(ParameterHelper.GraphBoltArgumentName.TEMP_DIR.toString())) {
            this.tempDirectory = (String) argValues.get(ParameterHelper.GraphBoltArgumentName.TEMP_DIR.toString());
        }

        // Did the user provide a cache directory location?
        if(argValues.containsKey(ParameterHelper.GraphBoltArgumentName.CACHE.toString())) {
            this.cacheDirectory = (String) argValues.get(ParameterHelper.GraphBoltArgumentName.CACHE.toString());
        }



        GraphStreamHandler.singleton = this;


        // Variables to tell GraphBolt whether to keep the cache, logging and temporary directories when program is terminating.
        this.keepingCacheDirectory = (boolean) argValues.get(ParameterHelper.GraphBoltArgumentName.KEEP_CACHE.toString());
        this.keepingLogDirectory = (boolean) argValues.get(ParameterHelper.GraphBoltArgumentName.KEEP_LOGS.toString());
        this.keepingTemporaryDirectory = (boolean) argValues.get(ParameterHelper.GraphBoltArgumentName.KEEP_TEMP_DIR.toString());
        this.saveFlinkPlans = (boolean) argValues.get(ParameterHelper.GraphBoltArgumentName.FLINK_SAVE_PLANS.toString());
        this.dumpingModel = (boolean) argValues.get(ParameterHelper.GraphBoltArgumentName.DUMP_MODEL.toString());
        this.saveFlinkJobOperatorStatistics = (boolean) argValues.containsKey(ParameterHelper.GraphBoltArgumentName.FLINK_SAVE_OPERATOR_STATS.toString());
        this.saveFlinkJobOperatorJSON = (boolean) argValues.containsKey(ParameterHelper.GraphBoltArgumentName.FLINK_SAVE_OPERATOR_JSON.toString());

        this.checkingPeriodicFullAccuracy = (boolean) argValues.containsKey(ParameterHelper.GraphBoltArgumentName.PERIODIC_FULL_ACCURACY_SET.toString());
    }

    /**
     * Call the normal constructor and set the GraphBolt execution strategy.
     *
     * @param executionStrategy The GraphBolt execution strategy.
     * @param argValues The user-provided program arguments.
     * @param f A vertex-parsing function.
     * @param graphAlgorithmName The name to identify the graph algorithm being processed by GraphBolt. Used to create directory names.
     */
    public GraphStreamHandler(Action executionStrategy, final Map<String, Object> argValues, final Function<String, Long> f, String graphAlgorithmName) {
        this(argValues, f, graphAlgorithmName);
        this.executionStrategy = executionStrategy;
    }

    /**
     * See attribute: {@link GraphStreamHandler#debugging}
     */
    protected String getCustomName() {
        return this.customName;
    }
    
    /**
     * Convert a string-based vertex identifier to Long.
     * @param s vertex identifier stored as a string.
     * @return the vertex identifier as a Long.
     */
    private Long parseStreamStringToken(String s) {
        return this.vertexIdTypeParser.apply(s);
    }

    /**
     * Launch Flink in either local or remote mode with appropriate defaults and user-provided parameters.
     * @param argValues The user-provided program arguments.
     */
    private void configureFlink(final Map<String, Object> argValues) {
        this.isRunningRemote =
                argValues.containsKey(ParameterHelper.GraphBoltArgumentName.SERVER_ADDRESS.toString()) &&
                        argValues.containsKey(ParameterHelper.GraphBoltArgumentName.SERVER_PORT.toString());

        if(isRunningRemote) {

            // We are going to execute in a Flink cluster in remote mode (not launching a local cluster).
            final String remoteAddress = (String) argValues.get(ParameterHelper.GraphBoltArgumentName.SERVER_ADDRESS.toString());
            final Integer remotePort = (Integer) argValues.get(ParameterHelper.GraphBoltArgumentName.SERVER_PORT.toString());

            // The .jar file which will be sent to the remote Flink. It the GraphBolt code.
            String jarFile = "";

            try {
                // Get root of the project in the file system and escape HTML-based URL characters.
                final File mvnRootFile = new File(GraphStreamHandler.class.getProtectionDomain()
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
                    if(line.startsWith("<artifactId>")) {
                        artifactId = line.substring(line.indexOf(">") + 1, line.lastIndexOf("<"));
                    }
                    if(line.startsWith("<version>")) {
                        version = line.substring(line.indexOf(">") + 1, line.lastIndexOf("<"));
                    }
                    if (artifactId != null && version != null) {
                        jarFile = "target/original-" + artifactId + "-" + version + packaging;
                        break;
                    }
                }
                scanner.close();

            } catch(FileNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }

            this.flinkJobManagerAddress = remoteAddress;
            this.flinkJobManagerPort = remotePort.toString();

            System.out.println(String.format("Flink ExecutionEnvironment connecting to:\t%s:%d.", remoteAddress, remotePort));
            System.out.println("Jar file:\t" + jarFile);
            this.env = ExecutionEnvironment.createRemoteEnvironment(remoteAddress, remotePort, jarFile);
        }
        else {
            System.out.println(String.format("Flink ExecutionEnvironment connecting locally."));


            Configuration conf = new Configuration();

            final boolean loadWebManager = (boolean) argValues.get(ParameterHelper.GraphBoltArgumentName.LOADING_WEB_MANAGER.toString());

            this.runningLocalFlinkWebUI = loadWebManager;


        /*
        conf.setString("web.log.path", this.loggingDirectory);
        conf.setString("jobmanager.rpc.address", "127.0.0.1");
        conf.setString("jobmanager.web.port", "8081-9000");
        conf.setString("query.server.ports", "30000-35000");
        conf.setString("query.proxy.ports", "35001-40000");
        conf.setString("jobmanager.rpc.port", "40001-45000");
        conf.setString("taskmanager.rpc.port", "45001-50000");
        conf.setString("taskmanager.data.port", "50001-55000");
        conf.setString("blob.server.port", "55001-60000");
        conf.setString("historyserver.web.port", "60001-62000");
*/

            this.flinkJobManagerAddress = "127.0.0.1";
            conf.setString(WebOptions.LOG_PATH.key(), this.loggingDirectory);
            conf.setString(JobManagerOptions.ADDRESS.key(), this.flinkJobManagerAddress);
            //conf.setString(JobManagerOptions.PORT.key(), "40001-45000");
            this.flinkJobManagerPort = "6123";//"8081";
            conf.setString(JobManagerOptions.PORT.key(), this.flinkJobManagerPort);
            //TODO: contribute this to Flink source (implement JOB_MANAGER_WEB_PORT_KEY in JobManagerOptions.
            conf.setString(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, "8081-9000");
            conf.setString(QueryableStateOptions.SERVER_PORT_RANGE.key(), "30000-35000");
            conf.setString(QueryableStateOptions.PROXY_PORT_RANGE.key(), "35001-40000");
            conf.setString(TaskManagerOptions.RPC_PORT.key(), "45001-50000");
            conf.setString(TaskManagerOptions.DATA_PORT.key(), "50001-55000");
            conf.setString(BlobServerOptions.PORT.key(), "55001-60000");
            conf.setString(HistoryServerOptions.HISTORY_SERVER_WEB_PORT.key(), "60001-62000");


            if(this.tempDirectory != null) {

                final String jobManagerTempDirectory = this.tempDirectory + FileSystems.getDefault().getSeparator() + "Logging/JobManager";
                final String taskManagerTempDirectory = this.tempDirectory + FileSystems.getDefault().getSeparator() + "Logging/TaskManager";

                conf.setString(CoreOptions.TMP_DIRS.key(), taskManagerTempDirectory);
                //conf.setString("taskmanager.tmp.dirs", taskManagerTempDirectory);
                conf.setString(WebOptions.TMP_DIR.key(), jobManagerTempDirectory);
                //conf.setString("jobmanager.web.tmpdir", jobManagerTempDirectory);
            }




            System.out.println("Loading WebManager on LocalEnvironment:\t" + loadWebManager);


            final LocalEnvironment lenv = (loadWebManager) ?
                    (LocalEnvironment) ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf) :
                    ExecutionEnvironment.createLocalEnvironment(conf);

            this.env = lenv;
        }

        final Integer parallelism = (Integer) argValues.get(ParameterHelper.GraphBoltArgumentName.PARALLELISM.toString());
        this.env.setParallelism(parallelism);

        this.env.getConfig()
                .enableClosureCleaner()
                .disableObjectReuse();
                //.enableObjectReuse(); //https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/index.html#object-reuse-enabled

        // See: https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/index.html#debugging
        if(argValues.containsKey(ParameterHelper.GraphBoltArgumentName.FLINK_PRINT_SYSOUT.toString())) {
            this.env.getConfig().enableSysoutLogging();
        }
        else {
            this.env.getConfig().disableSysoutLogging();
        }
    }

    /**
     * Configure GraphBolt's properties from the provided arguments.
     * @param argValues The user-provided program arguments.
     */
    private void configureGraphBolt(final Map<String, Object> argValues) {

        if(this.rootDirectory == null) {
            throw new IllegalStateException("Root directory must be set.");
        }
        if(this.datasetName == null) {
            throw new IllegalStateException("Dataset name must be set.");
        }


        System.out.println(String.format("Dataset name:\t%s", this.datasetName));
        System.out.println(String.format("Algorithm name:\t%s", this.algorithmName));

        // Check and build (if necessary) the cache directory for Apache Flink graph and result DataSet(s).
        if(this.cacheDirectory == null) {
            // The user did not provide a cache directory.
            if(! this.tempDirectory.endsWith(FileSystems.getDefault().getSeparator()))
                this.cacheDirectory = this.tempDirectory + FileSystems.getDefault().getSeparator() + "Cache";
            else
                this.cacheDirectory =  this.tempDirectory + "Cache";

            if(this.algorithmName.length() > 0)
                this.cacheDirectory += FileSystems.getDefault().getSeparator() + this.algorithmName;
        }
        else {
            //
            if(! this.cacheDirectory.endsWith(FileSystems.getDefault().getSeparator()))
                this.cacheDirectory += FileSystems.getDefault().getSeparator();

            if(this.algorithmName.length() > 0)
                this.cacheDirectory += this.algorithmName + FileSystems.getDefault().getSeparator();

        }

        this.cacheDirectory += this.getCustomName();
        this.cacheDirectory = this.cacheDirectory.replace(',', '.');


        // Create cache directory if it does not exist.
        try {
            Files.createDirectories(Paths.get(this.cacheDirectory));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Will graph models be dumped into disk?




        final StringJoiner modelJoiner = new StringJoiner(java.nio.file.FileSystems.getDefault().getSeparator())
                .add(this.rootDirectory)
                .add("Model");

        if(this.algorithmName.length() > 0)
            modelJoiner.add(this.algorithmName);

        this.modelDirectory = modelJoiner
                .add(this.getCustomName())
                .toString()
                .replace(',', '.');



        // Define the results directory.
        final StringJoiner resultsJoiner = new StringJoiner(java.nio.file.FileSystems.getDefault().getSeparator())
                .add(this.rootDirectory)
                .add("Results");

        if(this.algorithmName.length() > 0)
            resultsJoiner.add(this.algorithmName);

        this.resultsDirectory = resultsJoiner
                .add(this.getCustomName())
                .toString()
                .replace(',', '.');

        if(this.resultsDirectory == null) {
            throw new IllegalStateException("Results directory must be set.");
        }



        // Define the statistics directory.
        final StringJoiner statsJoiner = new StringJoiner(java.nio.file.FileSystems.getDefault().getSeparator())
                .add(this.rootDirectory)
                .add("Statistics");

        if(this.algorithmName.length() > 0)
            statsJoiner.add(this.algorithmName);


        statisticsDirectory = statsJoiner
                .add(this.getCustomName())
                .toString()
                .replace(',', '.');


        if(this.statisticsDirectory == null) {
            throw new IllegalStateException("Statistics directory must be set.");
        }


        //this.saveFlinkPlans = (boolean) argValues.get(ParameterHelper.GraphBoltArgumentName.FLINK_SAVE_PLANS.toString());

        System.out.println("Saving plans:\t" + this.saveFlinkPlans);

        if(this.saveFlinkPlans) {



            // Define the statistics directory.
            final StringJoiner planDirJoiner = new StringJoiner(java.nio.file.FileSystems.getDefault().getSeparator())
                    .add(this.rootDirectory)
                    .add("Plans");

            if(this.algorithmName.length() > 0)
                planDirJoiner.add(this.algorithmName);


            this.plansDirectory = planDirJoiner
                    .add(this.getCustomName())
                    .toString()
                    .replace(',', '.');

            // Create plans directory if it does not exist.
            final File directory = new File(this.plansDirectory);
            directory.mkdirs();

            System.out.println(String.format("Plans:\t\t%s", this.plansDirectory));

        }

        // Set and create logging directory.
        final StringJoiner logJoiner = new StringJoiner(FileSystems.getDefault().getSeparator())
                .add(this.rootDirectory)
                .add("Logging");

        if(this.algorithmName.length() > 0)
            logJoiner.add(this.algorithmName);


        this.loggingDirectory = logJoiner
                .add(this.getCustomName())
                .toString()
                .replace(',', '.');



        // Create logging directory if it does not exist.
        final File directory = new File(this.loggingDirectory);
        directory.mkdirs();

        // Configure log4j log path.
        final String log4jPath = this.loggingDirectory + FileSystems.getDefault().getSeparator() + String.format(this.getCustomName()).replace(',', '.') + ".log";

        try {


            final SimpleLayout layout = new SimpleLayout();
            final FileAppender fa = new FileAppender(layout, log4jPath,false);
            fa.setName("FlinkLogger");
            fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
            fa.setThreshold(Level.DEBUG);
            fa.setAppend(false); // truncate log if there was already a log file.
            fa.activateOptions();
            Logger.getRootLogger().addAppender(fa);
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println(String.format("Log4j log path:\t%s", log4jPath));


        System.out.println(String.format("Logging:\t%s", this.loggingDirectory));

        System.out.println("Keeping logs:\t" + this.keepingLogDirectory);

        System.out.println("Keeping cache:\t" + this.keepingCacheDirectory);
        if(this.keepingCacheDirectory) {
            if(this.cacheDirectory == null) {
                throw new IllegalStateException("Cache directory must be set.");
            }
        }

        try {

            // Create statistics directory if it does not exist.
            final java.nio.file.Path dirs = Files.createDirectories(Paths.get(this.statisticsDirectory));
            System.out.println(String.format("Statistics:\t%s", this.statisticsDirectory));

            // Create results directory if it does not exist.
            Files.createDirectories(Paths.get(this.resultsDirectory));
            System.out.println(String.format("Results:\t%s", this.resultsDirectory));

            // Create models directory if it does not exist and it is necessary.
            //if(dumpingModel) {
            Files.createDirectories(Paths.get(this.modelDirectory));
            System.out.println(String.format("Models:\t\t%s", this.modelDirectory));
            //}


            java.nio.file.Path file = dirs.resolve(this.datasetName + ".tsv"); //placed in the statistics directory
            if (!Files.exists(file)) {
                file = Files.createFile(file);
            }
            this.statisticsPrintStream = new PrintStream(file.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Configure streamer.
        this.streamPort = (Integer) argValues.get(ParameterHelper.GraphBoltArgumentName.STREAM_PORT.toString());
        this.updateStream = new SocketStreamProvider("localhost", this.streamPort);
        this.pendingUpdates = this.updateStream.getQueue();
    }

    public void start() throws Exception {

        // Define values such as the root output directory, statistics directory, logging, etc.
        this.configureGraphBolt(this.argValues);

        // Create an ExecutionEnvironment which might be local or remote.
        this.configureFlink(this.argValues);

        // Add generic GraphBolt statistics to the map.
        for(StatisticKeys statName: StatisticKeys.values()) {
            this.statisticsMap.put(statName.toString(), new ArrayList<>());
        }

        // Prepare GraphBolt-specific statistics.
        this.statOrder.add(StatisticKeys.EXECUTION_COUNTER.toString());
        this.statOrder.add(StatisticKeys.USED_STRATEGY.toString());
        this.statOrder.add(StatisticKeys.TIME_ACCUMULATED_BEFORE_UPDATE.toString());
        this.statOrder.add(StatisticKeys.TIME_TO_APPLY_UPDATES_ON_GRAPH.toString());
        this.statOrder.add(StatisticKeys.TIME_TO_APPLY_UPDATES_TOTAL.toString());
        this.statOrder.add(StatisticKeys.TIME_TO_COMPUTE_EXECUTION.toString());
        this.statOrder.add(StatisticKeys.TOTAL_VERTEX_COUNT.toString());
        this.statOrder.add(StatisticKeys.TOTAL_EDGE_COUNT.toString());


        this.graph = Graph
                .fromCsvReader(this.inputPath, this.env)
                .ignoreCommentsEdges("#")
                .fieldDelimiterEdges("\t")
                .keyType(Long.class);


        this.graphUpdateTracker = new GraphUpdateTracker<>(this.graph);


        // Reminder: this is abstract. User-specified initialization.
        init();

        // If the algorithm is using a summary structure, initialize its statistics file.
        if(this.model != null) {
            this.model.initStatistics(this.statisticsDirectory);
        }

        // Only printing the cache directory here because the abstract init() may have redefined it.
        System.out.println(String.format("Cache:\t\t%s", this.cacheDirectory));

        // Add custom user statistics to the map.
        for(String statName: this.statisticsMap.keySet()) {
            if( ! statOrder.contains(statName))
                this.statOrder.add(statName);
        }

        // Print out the header of the statistics file.
        final StringJoiner statJoiner = new StringJoiner(";");
        for(String stat: this.statOrder) {
            statJoiner.add(stat);
        }

        final String statLine = statJoiner.toString();
        this.statisticsPrintStream.println(statLine);
        this.statisticsPrintStream.flush();


        if(this.debugging) {
            System.out.println(this.argValues);
        }




        new Thread(this.updateStream, "GraphBolt Stream Consumer").start();
        new Thread(this, "GraphBolt Main Loop").start();
    }

    protected Long applyUpdates() throws Exception {

    	// Must copy the added edges into the DataSet degreeDataset
        this.edgeInputFormat.setFilePath(this.cacheDirectory + "/edges" + ((this.iteration - 1) % this.storedIterations));
        this.graph = Graph.fromTuple2DataSet(this.env.createInput(this.edgeInputFormat), this.env);


        final GraphUpdates<Long, NullValue> updates = this.graphUpdateTracker.getGraphUpdates();

    	// Check new vertices.
        if (!updates.verticesToAdd.isEmpty()) {
            List<Vertex<Long, NullValue>> vertices = updates.verticesToAdd.stream()
                    .map(id -> new Vertex<>(id, NullValue.getInstance()))
                    .distinct()
                    .collect(Collectors.toList());
            this.graph = this.graph.addVertices(vertices);
        }

        // Check new edges.
        if (!updates.edgesToAdd.isEmpty()) {
            ArrayList<Edge<Long, NullValue>> edgesToAdd = new ArrayList<>(updates.edgesToAdd);//updates.edgesToAdd);
        	this.graph = this.graph.addEdges(edgesToAdd);
        }

        // Check vertices to be deleted.
        if (!updates.verticesToRemove.isEmpty()) {
        	this.graph = this.graph.removeVertices(updates.verticesToRemove.stream()
                    .map(id -> new Vertex<>(id, NullValue.getInstance()))
                    .distinct()
                    .collect(Collectors.toList()));
        }

        // Check edges to delete.
        if (!updates.edgesToRemove.isEmpty()) {
            System.out.println("EDGES TO REMOVE");
        	this.graph = this.graph.removeEdges(new ArrayList<>(updates.edgesToRemove));
        }


        this.edgeOutputFormat.setOutputFilePath(new Path(this.cacheDirectory + "/edges" + (this.iteration % this.storedIterations)));
        this.graph
                .getEdgeIds()
                .output(this.edgeOutputFormat)
                .name("GraphStreamHandler - write updated graph to disk.");

        // Trigger Flink execution for the graph update and disk cache edge spill.
        final Long numberOfVertices = this.graph.numberOfVertices();
        final Long numberOfEdges = this.graph.numberOfEdges();
        final JobExecutionResult r = env.getLastJobExecutionResult();




        statisticsMap.get(StatisticKeys.TOTAL_VERTEX_COUNT.toString()).add(numberOfVertices);
        statisticsMap.get(StatisticKeys.TOTAL_EDGE_COUNT.toString()).add(numberOfEdges);

        System.out.format("%d;%d;%d;%d;%d%n",
                this.iteration,
                this.graphUpdateTracker.getAccumulatedTime(),
                r.getNetRuntime(TimeUnit.MILLISECONDS),
                numberOfVertices,
                numberOfEdges);

        return r.getNetRuntime(TimeUnit.MILLISECONDS);

    }

    protected void registerEdgeDelete(final String[] split) {
        final Edge<Long, NullValue> edge = parseEdge(split);
        //System.out.println("Deleting edge: " + edge.toString());
    	this.graphUpdateTracker.removeEdge(edge);
    }

    protected void registerEdgeAdd(final String[] split) {
        Edge<Long, NullValue> edge = parseEdge(split);
        this.graphUpdateTracker.addEdge(edge);
    }

    private Edge<Long, NullValue> parseEdge(final String[] data) {
        assert data.length == ELEMENT_COUNT_PER_UPDATE_MSG;

        Long source = this.parseStreamStringToken(data[UPDATE_SOURCE_VERTEX_INDEX]);
        Long target = this.parseStreamStringToken(data[UPDATE_TARGET_VERTEX_INDEX]);

        return new Edge<>(source, target, NullValue.getInstance());
    }

    // GraphBolt resource cleanup.
    protected void cleanup() throws IOException {

        // Close the statistics stream.
        this.statisticsPrintStream.close();

        if ( ! this.keepingLogDirectory) {
            final java.nio.file.Path logPathToken = Paths.get(this.loggingDirectory);
            GraphUtils.deleteFileOrFolder(logPathToken);
        }

        if ( ! this.keepingCacheDirectory) {
            final java.nio.file.Path cachePath = Paths.get(this.cacheDirectory);
            GraphUtils.deleteFileOrFolder(cachePath);
        }

        if ( ! this.keepingTemporaryDirectory) {
            //TODO: check if necessary to delete files here...
        }
    }

    // GraphBolt main loop.
    @Override
    public void run() {

        boolean running = true;

        while (running) {
            try {
                final String updateString = pendingUpdates.take();

                

                final String[] split = updateString.trim().split("\\s+");

                switch (split[0]) {
                    case "END": {
                        this.cleanup();
                        if(this.model != null) {
                            this.model.cleanup();
                        }
                        running = false;
                        break;
                    }
                    case "A": {
                        //System.out.println(updateString);
                        this.registerEdgeAdd(split);
                        break;
                    }
                    case "D": {
                        //System.out.println(updateString);
                        this.registerEdgeDelete(split);
                        break;
                    }
                    case "Q": {

                        this.snapshotCtr++;

                        // Each 'Q' message counts as a new iteration.
                        this.iteration++;

                        statisticsMap.get(StatisticKeys.EXECUTION_COUNTER.toString()).add(this.iteration);

                        final long querySetupStart = System.nanoTime();

                        // Normalmente o infoMap do graphUpdateTracker teria o in e out degree de todos os vértices
                        final GraphUpdates<Long, NullValue> graphUpdates = this.graphUpdateTracker.getGraphUpdates();
                        final GraphUpdateStatistics statistics = this.graphUpdateTracker.getUpdateStatistics();

                        final boolean needToApplyUpdates = this.beforeUpdates(graphUpdates, statistics);

                        if (needToApplyUpdates) {
                            // Incorporate added/removed graph elements into the current Gelly graph.

                            // Build a new graph by reading the old one from disk and also adding the graph changes.
                            final Long updateTime = this.applyUpdates();
                            statisticsMap.get(StatisticKeys.TIME_TO_APPLY_UPDATES_ON_GRAPH.toString()).add(updateTime);

                            // Get total tracked update time (time spent registering edge/vertex additions/deletions outside Flink).
                            final Long accumulatedTime = this.graphUpdateTracker.getAccumulatedTime();
                            statisticsMap.get(StatisticKeys.TIME_ACCUMULATED_BEFORE_UPDATE.toString()).add(accumulatedTime);

                            this.graphUpdateTracker.resetUpdates();
                        }

                        final Action action = this.onQuery(this.iteration, updateString, this.graph, graphUpdates, statistics, this.graphUpdateTracker.getUpdateInfos());

                        this.executionStrategy = action;

                        statisticsMap.get(StatisticKeys.USED_STRATEGY.toString()).add((long)action.ordinal());

                        final long querySetupEnd = System.nanoTime();
                        final long querySetupTotalTime = querySetupEnd - querySetupStart;

                        statisticsMap.get(StatisticKeys.TIME_TO_APPLY_UPDATES_TOTAL.toString()).add(TimeUnit.MILLISECONDS.convert(querySetupTotalTime, TimeUnit.NANOSECONDS));

                        System.out.println(String.format("Update integration (%d-ith) execution time: %d.%d",
                                this.iteration,
                                TimeUnit.SECONDS.convert(querySetupTotalTime, TimeUnit.NANOSECONDS),
                                TimeUnit.MILLISECONDS.convert(querySetupTotalTime, TimeUnit.NANOSECONDS) % 1000));


                        System.out.println("Computation #" + this.iteration + " - " + action.toString());

                        Long executionTime = -1L;

                        switch (action) {
                            case AUTOMATIC:
                                executionTime = this.executeAutomatic();
                                break;
                            case COMPUTE_APPROXIMATE:
                                executionTime = this.executeApproximate();
                                break;
                            case COMPUTE_EXACT:
                                executionTime = this.executeExact();
                                break;
                        }



                        // Need to check model statistics (and prepare model write to disk if the user required it).
                        if(this.model != null)
                            this.model.registerStatistics(this.iteration, this.env);

                        this.onQueryResult(this.iteration, updateString, action, this.graph);

                        statisticsMap.get(StatisticKeys.TIME_TO_COMPUTE_EXECUTION.toString()).add(executionTime);

                        // Print all statistics of this iteration as a line in the statistics file.
                        final StringJoiner statJoiner = new StringJoiner(";");
                        for(String stat: this.statOrder) {
                            final String statVal = this.statisticsMap.get(stat).get(this.statisticsMap.get(stat).size() - 1).toString();
                            statJoiner.add(statVal);
                        }

                        final String statLine = statJoiner.toString();
                        this.statisticsPrintStream.println(statLine);
                        this.statisticsPrintStream.flush();




                        if(this.timeToSnapshot()) {
                            this.snapshotCtr = 0;
                        }


                        this.graphUpdateTracker.resetAll();

                        break;
                    }

                }




            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }


    }

    protected Boolean timeToSnapshot() {
        return this.dumpingModel && (this.snapshotCtr == this.snapshotFrequency);
    }

    public Long getIteration() {
        return this.iteration;
    }

    public ExecutionEnvironment getExecutionEnvironment() {
        return this.env;
    }

    // GraphBolt stream UDFs.
    protected abstract boolean beforeUpdates(final GraphUpdates<Long, NullValue> updates, final GraphUpdateStatistics statistics);

    protected abstract Action onQuery(final Long id, final String query, final Graph<Long, NullValue, NullValue> graph,
                                      final GraphUpdates<Long, NullValue> updates,
                                      final GraphUpdateStatistics statistics,
                                      final Map<Long, GraphUpdateTracker.UpdateInfo> updateInfos);

    protected abstract void onQueryResult(
            final Long id,
            final String query,
            final Action action,
            final Graph<Long, NullValue, NullValue> graph);

    // GraphBolt execution strategy UDFs.
    public abstract void init() throws Exception;

    //TODO: LV sugeriu incorporar estas UDFs numa interface GraphModel
    protected abstract Long executeExact() throws Exception;

    protected abstract Long executeApproximate() throws Exception;

    protected abstract Long executeAutomatic() throws Exception;

    /**
     *
     * @return Get the current directory for storing plans.
     */
    public String getPlansDirectory() {
        return this.plansDirectory;
    }

    /**
     * Get the current Flink JobManager port as a String.
     */
    public String getFlinkJobManagerPort() {
        return this.flinkJobManagerPort;
    }

    public boolean runningLocalFlinkWebUI() {
        return this.runningLocalFlinkWebUI;
    };

    public enum StatisticKeys {
        // Number of executions since the stream started.
        EXECUTION_COUNTER("execution_count"),
        // GraphBolt execution strategy (exact, approximate, automatic, etc.)
        USED_STRATEGY("used_strategy"),
        // Accumulated time before inserting the updates into the graph.
        TIME_ACCUMULATED_BEFORE_UPDATE("accumulated_time_before_applying_update"),
        // Time taken to ingest updates into the graph.
        TIME_TO_APPLY_UPDATES_ON_GRAPH("graph_update_time"),
        // Total time to process updates (including ingesting in the graph)
        TIME_TO_APPLY_UPDATES_TOTAL("total_update_time"),
        // Time taken to perform computations.
        TIME_TO_COMPUTE_EXECUTION("computation_time"),
        TOTAL_VERTEX_COUNT("total_vertex_num"),
        TOTAL_EDGE_COUNT("total_edge_num");



        private final String text;

        /**
         * @param text
         */
        StatisticKeys(final String text) {
            this.text = text;
        }

        /* (non-Javadoc)
         * @see java.lang.Enum#toString()
         */
        @Override
        public String toString() {
            return text;
        }
    }
    // Possible algorithm execution strategies.
    public enum Action {
        //REPEAT_LAST_ANSWER,
        COMPUTE_APPROXIMATE,
        COMPUTE_EXACT,
        AUTOMATIC
    }
}


