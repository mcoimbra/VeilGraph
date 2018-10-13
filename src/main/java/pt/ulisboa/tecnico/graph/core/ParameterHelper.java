package pt.ulisboa.tecnico.graph.core;

import org.apache.commons.cli.*;
import pt.ulisboa.tecnico.graph.algorithm.pagerank.PageRankParameterHelper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ParameterHelper {

    public enum GraphBoltArgumentName {
        /**
         * Should GraphBolt's license be printed when running?
         */
        LICENSE("license"),

        /**
         * Set debug mode.
         */
        DEBUG_SHORT("dbg"), DEBUG("debug"),

        /**
         * Number of algorithm executions performed by GraphBolt.
         * Should match the number of chunks sent by the graph update stream.
         */
        EXECUTION_LIMIT("execution_limit"),

        /**
         * Dumping graph summarization model structures to disk?
         */
        DUMP_MODEL("dump"),

        /**
         * Base input graph over which a complete execution is initially executed (does not count towards @link{pt.ulisboa.tecnico.graph.core.ParameterHelper.GraphBoltArgumentName.EXECUTION_LIMIT}).
         */
        INPUT_FILE_SHORT("i"), INPUT_FILE("input-graph"),

        /**
         * Directory where GraphBolt sub-directories will be created ("Statistics", "Results").
         */
        OUTPUT_DIR_SHORT("o"), OUTPUT_DIR("output-directory"),

        /**
         * Level of parallelism passed on from GraphBolt to the Flink JobManager.
         */
        PARALLELISM_SHORT("para"), PARALLELISM("parallelism"),

        /**
         * Port to listen for the graph update stream.
         */
        STREAM_PORT_SHORT("sp"), STREAM_PORT("stream-port"),

        /**
         * Should Flink load the WebManager when running as a MiniCluster?
         */
        LOADING_WEB_MANAGER("web"),

        /**
         * Port for the Flink cluster JobManager.
         * Used to connect to a remote cluster or to define the port of a local Flink MiniCluster instance.
         */
        SERVER_PORT_SHORT("p"), SERVER_PORT("port"),

        /**
         * Address for the Flink cluster JobManager.
         * Used to connect to a remote cluster or to define the address of a local Flink MiniCluster instance.
         */
        SERVER_ADDRESS_SHORT("ip"), SERVER_ADDRESS("address"),

        /**
         * Keeping Flink logs?
         */
        KEEP_LOGS("keep_logs"),

        /**
         * Should Flink's internal dataflow execution information be printed to stdout?
         */
        FLINK_PRINT_SYSOUT("print_flink_sysout"),

        /**
         * Saving Flink execution plans as JSON?
         */
        FLINK_SAVE_PLANS("save_flink_plans"),

        /**
         * Saving the operator stats?
         */
        FLINK_SAVE_OPERATOR_STATS("save_flink_operator_stats"),

        /**
         * Saving Flink dataflow operator statistics to disk?
         */
        FLINK_SAVE_OPERATOR_JSON("save_flink_operator_json"),

        /**
         * Directory for intermediate DataSet dumps in between executions.
         */
        CACHE("cache"),

        /**
         * Should we keep the cache directory after finishing?
         */
        KEEP_CACHE("keep_cache"),

        /**
         * Directory for Flink internal temporary files.
         */
        TEMP_DIR("temp"),

        /**
         * Should we keep the temporary directory after finishing?
         */
        KEEP_TEMP_DIR("keep_temp_dir"),

        /**
         * Are we simulating with a stream containing edge deletions?
         */
        DELETING_EDGES("with_deletions"),

        /**
         * Should the graph algorithm (chosen by the user) results be periodically written to disk in their totality?
         * For example, with PageRankStreamHandler, only the top PAGERANK_SIZE_SHORT/PAGERANK_SIZE vertex ranks are written (see {@link pt.ulisboa.tecnico.graph.algorithm.pagerank.PageRankParameterHelper}).
         */
        PERIODIC_FULL_ACCURACY_SET("periodic_full_accuracy");

        private final String text;
        GraphBoltArgumentName(final String text) {
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

    protected final Options options = new Options();
    protected final Map<String, Object> argValues = new HashMap<>();

    protected final CommandLineParser parser = new DefaultParser();
    private final HelpFormatter formatter = new HelpFormatter();
    protected CommandLine cmd = null;

    public Map<String, Object> getParameters() {
        return this.argValues;
    }


    /**
     * By default, there is no suffix to append.
     * This method should be overridden if the programmer wants to customize the directory name for statistics, results, etc.
     * @return an empty string for when it is not overridden.
     */
    public String getFileSuffix() {
        return "";
    }

    protected void parseValues(final String[] args) {
        // Set values

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        // Check if the user asked to print the license.
        if (cmd.hasOption(GraphBoltArgumentName.LICENSE.toString())) {
            final String relativeToLicense = File.separator + ".." + File.separator + ".." + File.separator;
            final String licensePath = PageRankParameterHelper.class.getProtectionDomain().getCodeSource().getLocation().getPath() + relativeToLicense + "LICENSE-2.0.txt";
            try (BufferedReader br = new BufferedReader(new FileReader(licensePath))) {
                String line;
                System.out.println("Printing program license:\n");
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }
            } catch (FileNotFoundException e) {
                System.err.println("Error: the license file supplied with this program was not found...");
                System.err.println("Printing stack trace and exiting.");
                e.printStackTrace();
                System.exit(1);
            } catch (IOException e) {
                System.err.println("Error: something went wrong when reading the license file... Exiting.");
                System.err.println("Printing stack trace and exiting.");
                e.printStackTrace();
                System.exit(1);
            }
        }

        // If no output directory was given, set it to the system's temporary Java directory.
        String outDirPath = System.getProperty("java.io.tmpdir");
        if(cmd.hasOption(GraphBoltArgumentName.OUTPUT_DIR.toString())) {
            outDirPath = cmd.getOptionValue(GraphBoltArgumentName.OUTPUT_DIR.toString());
            File outDirHandle = new File(outDirPath).getAbsoluteFile();
            if(!outDirHandle.exists()) {
                boolean result = false;
                try{
                    result = outDirHandle.mkdir();
                }
                catch(SecurityException se){
                    System.err.println("Error creating output directory:" + outDirPath);
                    se.printStackTrace();
                    System.exit(1);
                }
                if(result) {
                    System.out.println("Created output directory:\t" + outDirPath);
                }
                else {
                    System.out.println("Create directory failed:\t " + outDirPath);
                    System.exit(1);
                }
            }
            else if(!outDirHandle.isDirectory()) {
                System.out.println("The given output directory path was a file but should be a directory:");
                System.out.println(outDirPath);
                System.out.println("Exiting.");
                System.exit(1);
            }
        }


        if(	 cmd.hasOption(GraphBoltArgumentName.EXECUTION_LIMIT.toString()))  {
            final int executionLimit = Integer.parseInt(cmd.getOptionValue(GraphBoltArgumentName.EXECUTION_LIMIT.toString()));

            if(executionLimit <= 0)
                throw new IllegalArgumentException(GraphBoltArgumentName.EXECUTION_LIMIT.toString() + " must be a positive integer.");

            argValues.put(GraphBoltArgumentName.EXECUTION_LIMIT.toString(), executionLimit);
        }
        else {
            argValues.put(GraphBoltArgumentName.EXECUTION_LIMIT.toString(), -1);
        }

        if(	 cmd.hasOption(GraphBoltArgumentName.TEMP_DIR.toString()))  {
            final String tempDir = cmd.getOptionValue(GraphBoltArgumentName.TEMP_DIR.toString());

            final File file = new File(tempDir);
            if (! file.exists() || ! file.isDirectory())
                throw new IllegalArgumentException(GraphBoltArgumentName.TEMP_DIR.toString() + " must be a valid file path.");


            argValues.put(GraphBoltArgumentName.TEMP_DIR.toString(), tempDir);
        }


        argValues.put(GraphBoltArgumentName.OUTPUT_DIR.toString(), outDirPath);


        if(cmd.hasOption(GraphBoltArgumentName.PARALLELISM.toString())) {
            final Integer parallelism = Integer.parseInt(cmd.getOptionValue(GraphBoltArgumentName.PARALLELISM.toString()));

            if(parallelism <= 0)
                throw new IllegalArgumentException(GraphBoltArgumentName.PARALLELISM.toString() + " must be a positive integer.");

            argValues.put(GraphBoltArgumentName.PARALLELISM.toString(), parallelism);
        }
        else {
            argValues.put(GraphBoltArgumentName.PARALLELISM.toString(), 1);
        }


        if(cmd.hasOption(GraphBoltArgumentName.STREAM_PORT.toString())) {
            final Integer streamPort = Integer.parseInt(cmd.getOptionValue(GraphBoltArgumentName.STREAM_PORT.toString()));

            if(streamPort <= 0)
                throw new IllegalArgumentException(GraphBoltArgumentName.STREAM_PORT.toString() + " must be a positive integer.");

            argValues.put(GraphBoltArgumentName.STREAM_PORT.toString(), streamPort);
        }


        if(cmd.hasOption(GraphBoltArgumentName.SERVER_PORT.toString())) {
            final Integer flinkServerPort = Integer.parseInt(cmd.getOptionValue(GraphBoltArgumentName.SERVER_PORT.toString()));

            if(flinkServerPort <= 0)
                throw new IllegalArgumentException(GraphBoltArgumentName.SERVER_PORT.toString() + " must be a positive integer.");

            argValues.put(GraphBoltArgumentName.SERVER_PORT.toString(), flinkServerPort);
        }

        if(cmd.hasOption(GraphBoltArgumentName.SERVER_ADDRESS.toString())) {
            final String flinkServerAddress = cmd.getOptionValue(GraphBoltArgumentName.SERVER_ADDRESS.toString());
            argValues.put(GraphBoltArgumentName.SERVER_ADDRESS.toString(), flinkServerAddress);
        }

        final String inputPath = cmd.getOptionValue(GraphBoltArgumentName.INPUT_FILE.toString());

        final File file = new File(inputPath);
        if (! file.exists() || file.isDirectory())
            throw new IllegalArgumentException(GraphBoltArgumentName.INPUT_FILE.toString() + " must be a valid file path.");

        argValues.put(GraphBoltArgumentName.INPUT_FILE.toString(), inputPath);

        // Check argument flags.
        argValues.put(GraphBoltArgumentName.FLINK_PRINT_SYSOUT.toString(), cmd.hasOption(GraphBoltArgumentName.FLINK_PRINT_SYSOUT.toString()));

        argValues.put(GraphBoltArgumentName.FLINK_SAVE_PLANS.toString(), cmd.hasOption(GraphBoltArgumentName.FLINK_SAVE_PLANS.toString()));

        argValues.put(GraphBoltArgumentName.FLINK_SAVE_OPERATOR_STATS.toString(), cmd.hasOption(GraphBoltArgumentName.FLINK_SAVE_OPERATOR_STATS.toString()));

        argValues.put(GraphBoltArgumentName.FLINK_SAVE_OPERATOR_JSON.toString(), cmd.hasOption(GraphBoltArgumentName.FLINK_SAVE_OPERATOR_JSON.toString()));

        argValues.put(GraphBoltArgumentName.LOADING_WEB_MANAGER.toString(), cmd.hasOption(GraphBoltArgumentName.LOADING_WEB_MANAGER.toString()));

        argValues.put(GraphBoltArgumentName.PERIODIC_FULL_ACCURACY_SET.toString(), cmd.hasOption(GraphBoltArgumentName.PERIODIC_FULL_ACCURACY_SET.toString()));

        argValues.put(GraphBoltArgumentName.DELETING_EDGES.toString(), cmd.hasOption(GraphBoltArgumentName.DELETING_EDGES.toString()));

        argValues.put(GraphBoltArgumentName.KEEP_LOGS.toString(), cmd.hasOption(GraphBoltArgumentName.KEEP_LOGS.toString()));

        argValues.put(GraphBoltArgumentName.KEEP_CACHE.toString(), cmd.hasOption(GraphBoltArgumentName.KEEP_CACHE.toString()));

        argValues.put(GraphBoltArgumentName.KEEP_TEMP_DIR.toString(), cmd.hasOption(GraphBoltArgumentName.KEEP_TEMP_DIR.toString()));

        argValues.put(GraphBoltArgumentName.DUMP_MODEL.toString(), cmd.hasOption(GraphBoltArgumentName.DUMP_MODEL.toString()));

        argValues.put(GraphBoltArgumentName.DEBUG.toString(), cmd.hasOption(GraphBoltArgumentName.DEBUG.toString()));

        if(cmd.hasOption(GraphBoltArgumentName.CACHE.toString())) {
            final String cachePath = cmd.getOptionValue(GraphBoltArgumentName.CACHE.toString());

            final File cacheFile = new File(cachePath);
            if (! cacheFile.exists() || ! cacheFile.isDirectory())
                throw new IllegalArgumentException(GraphBoltArgumentName.CACHE.toString() + " must be a valid file path. Provided: " + cachePath);

            argValues.put(GraphBoltArgumentName.CACHE.toString(), cachePath);
        }

        argValues.put(GraphBoltArgumentName.DEBUG.toString(), cmd.hasOption(GraphBoltArgumentName.DEBUG.toString()));
        if(cmd.hasOption(GraphBoltArgumentName.DEBUG.toString())) {
            for(Map.Entry<String, Object> param : argValues.entrySet()) {
                System.out.println(param.getKey() + "\t" + param.getValue().toString());
            }
            System.out.println("\n");
        }
    }

    protected void setupCLIOptions() {
        // Program options.
        final Option licenseOption = new Option(GraphBoltArgumentName.LICENSE.toString(), false, "output this program's license.");
        licenseOption.setRequired(false);
        options.addOption(licenseOption);

        // GraphBolt parameters.
        final Option cacheOption = new Option(GraphBoltArgumentName.CACHE.toString(), true, "path to GraphBolt cache directory.");
        cacheOption.setRequired(false);
        options.addOption(cacheOption);

        final Option tempOption = new Option(GraphBoltArgumentName.TEMP_DIR.toString(), true, "path to directory to use for temporary files.");
        tempOption.setRequired(false);
        options.addOption(tempOption);

        final Option outputDirOption = new Option(GraphBoltArgumentName.OUTPUT_DIR_SHORT.toString(), GraphBoltArgumentName.OUTPUT_DIR.toString(),
                true, "output directory for statistics, results and additional things.");
        outputDirOption.setRequired(false);
        options.addOption(outputDirOption);

        // Apache Flink network job submission configuration.
        final Option serverPortOption = new Option(GraphBoltArgumentName.SERVER_PORT_SHORT.toString(), GraphBoltArgumentName.SERVER_PORT.toString(),
                true, "port of the Apache Flink JobManager.");
        serverPortOption.setRequired(false);
        options.addOption(serverPortOption);

        final Option serverAddressOption = new Option(GraphBoltArgumentName.SERVER_ADDRESS_SHORT.toString(), GraphBoltArgumentName.SERVER_ADDRESS.toString(),
                true, "address of the Apache Flink JobManager.");
        serverAddressOption.setRequired(false);
        options.addOption(serverAddressOption);

        final Option executionLimitOption = new Option(GraphBoltArgumentName.EXECUTION_LIMIT.toString(),
                true, "maximum number of updates to employ.");
        executionLimitOption.setRequired(false);
        options.addOption(executionLimitOption);

        final Option parallelismOption = new Option(GraphBoltArgumentName.PARALLELISM_SHORT.toString(), GraphBoltArgumentName.PARALLELISM.toString(),
                true, "parallelism level for Apache Flink dataflow operators.");
        parallelismOption.setRequired(false);
        options.addOption(parallelismOption);


        final Option streamPortOption = new Option(GraphBoltArgumentName.STREAM_PORT_SHORT.toString(), GraphBoltArgumentName.STREAM_PORT.toString(),
                true, "port for Apache Flink to listen to graph updates in a stream.");
        streamPortOption.setRequired(false);
        options.addOption(streamPortOption);

        final Option inputFileOption = new Option(GraphBoltArgumentName.INPUT_FILE_SHORT.toString(), GraphBoltArgumentName.INPUT_FILE.toString(),
                true, "path to the input graph.");
        inputFileOption.setRequired(true);
        options.addOption(inputFileOption);


        final Option flinkSysOutOption = new Option(GraphBoltArgumentName.FLINK_PRINT_SYSOUT.toString(),
                false, "should Flink standard output be active?");
        flinkSysOutOption.setRequired(false);
        options.addOption(flinkSysOutOption);

        final Option flinkSavePlansOption = new Option(GraphBoltArgumentName.FLINK_SAVE_PLANS.toString(),
                false, "should Flink plans be saved before execute() calls?");
        flinkSavePlansOption.setRequired(false);
        options.addOption(flinkSavePlansOption);

        final Option flinkSaveOperatorStatisticsOption = new Option(GraphBoltArgumentName.FLINK_SAVE_OPERATOR_STATS.toString(),
                false, "should Flink job operator statistics be saved after execute() calls?");
        flinkSaveOperatorStatisticsOption.setRequired(false);
        options.addOption(flinkSaveOperatorStatisticsOption);

        final Option flinkSaveOperatorJSONOption = new Option(GraphBoltArgumentName.FLINK_SAVE_OPERATOR_JSON.toString(),
                false, "should Flink job operator JSON be saved after execute() calls?");
        flinkSaveOperatorJSONOption.setRequired(false);
        options.addOption(flinkSaveOperatorJSONOption);


        final Option edgeDeletionOption = new Option(GraphBoltArgumentName.DELETING_EDGES.toString(),
                false, "are we deleting edges?");
        edgeDeletionOption.setRequired(false);
        options.addOption(edgeDeletionOption);



        final Option debugOption = new Option(GraphBoltArgumentName.DEBUG_SHORT.toString(), GraphBoltArgumentName.DEBUG.toString(),
                false, "output debug information.");
        debugOption.setRequired(false);
        options.addOption(debugOption);

        final Option usingWebManagerOption = new Option(GraphBoltArgumentName.LOADING_WEB_MANAGER.toString(),
                false, "should the Flink web manager be started?");
        usingWebManagerOption.setRequired(false);
        options.addOption(usingWebManagerOption);


        final Option checkingPeriodically = new Option(GraphBoltArgumentName.PERIODIC_FULL_ACCURACY_SET.toString(),
                false, "should all the results be checked periodically?");
        checkingPeriodically.setRequired(false);
        options.addOption(checkingPeriodically);
        

        final Option keepLogDirectoryOption = new Option(GraphBoltArgumentName.KEEP_LOGS.toString(),
                false, "should Apache Flink logs be deleted?");
        keepLogDirectoryOption.setRequired(false);
        options.addOption(keepLogDirectoryOption);

        final Option keepCacheDirectoryOption = new Option(GraphBoltArgumentName.KEEP_CACHE.toString(),
                false, "should the GraphBolt cache directory be deleted?");
        keepCacheDirectoryOption.setRequired(false);
        options.addOption(keepCacheDirectoryOption);

        final Option keepTempDirOption = new Option(GraphBoltArgumentName.KEEP_TEMP_DIR.toString(),
                false, "should the Apache Flink temporary directory be deleted?");
        keepTempDirOption.setRequired(false);
        options.addOption(keepTempDirOption);


        final Option dumpingSummaryOption = new Option(GraphBoltArgumentName.DUMP_MODEL.toString(),
                false, "should the GraphBolt summary structures be saved to disk?");
        dumpingSummaryOption.setRequired(false);
        options.addOption(dumpingSummaryOption);


    }
}
