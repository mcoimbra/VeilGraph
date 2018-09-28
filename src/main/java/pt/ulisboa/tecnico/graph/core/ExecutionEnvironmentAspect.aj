package pt.ulisboa.tecnico.graph.core;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import pt.ulisboa.tecnico.graph.stream.GraphStreamHandler;
import org.json.JSONObject;
import org.json.JSONArray;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Helpful information on AspectJ.
 *
 * https://www.eclipse.org/aspectj/doc/released/
 * https://www.eclipse.org/aspectj/doc/released/progguide/starting-aspectj.html#the-dynamic-join-point-model
 * https://www.eclipse.org/aspectj/doc/released/quick.pdf
*/

public aspect ExecutionEnvironmentAspect {


    //private transient PrintStream jobStatsPrintStream = null;


    /*
    private String plansDirectory;
    private String statisticsDirectory;
    private Long iteration;
    private boolean saveFlinkPlans = false;
    private boolean saveFlinkJobOperatorStatistics = false;

    before(GraphStreamHandler gsh, String plansDirectory): set(String GraphStreamHandler.plansDirectory) && args(plansDirectory) && target(gsh) {
        this.plansDirectory = plansDirectory;
    }

    before(GraphStreamHandler gsh, String statisticsDirectory): set(String GraphStreamHandler.statisticsDirectory) && args(statisticsDirectory) && target(gsh) {
        this.statisticsDirectory = statisticsDirectory;
    }

    before(GraphStreamHandler gsh, Long iteration): set(Long GraphStreamHandler.iteration) && args(iteration) && target(gsh) {
        this.iteration = iteration;
    }


    before(GraphStreamHandler gsh, boolean saveFlinkJobOperatorStatistics): set(boolean GraphStreamHandler.saveFlinkJobOperatorStatistics) && args(saveFlinkJobOperatorStatistics) && target(gsh) {
        this.saveFlinkJobOperatorStatistics = saveFlinkJobOperatorStatistics;
    }


    before(GraphStreamHandler gsh, boolean saveFlinkPlans): set(boolean GraphStreamHandler.saveFlinkPlans) && args(saveFlinkPlans) && target(gsh) {
        this.saveFlinkPlans = saveFlinkPlans;
    }

    */

    pointcut flinkJobExecuteCall(ExecutionEnvironment env):
            target(env) &&
            (call(JobExecutionResult ExecutionEnvironment.execute()) || call(JobExecutionResult ExecutionEnvironment.execute(String)));

    /**
     * Write the Flink execution plan before actually executing.
     */
    before(ExecutionEnvironment env): flinkJobExecuteCall(env) {

        final boolean savingFlinkPlans = GraphStreamHandler.getInstance().savingFlinkPlans();
        if (savingFlinkPlans) {
            try {
                    final String plansDirectory = GraphStreamHandler.getInstance().getPlansDirectory();
                    final Long iteration = GraphStreamHandler.getInstance().getIteration();

                    final String planJSON = env.getExecutionPlan();

                    //TODO: instead of a hash, the plan name could be something more human-friendly.
                    final String planFileName = String.format(plansDirectory + "/plan-%d_%d.json", iteration, planJSON.hashCode());

                    System.out.println("Plan:\t\t" + planFileName);
                    final BufferedWriter out = new BufferedWriter(new FileWriter(planFileName));
                    out.write(planJSON);
                    out.close();

                } catch(IOException e){
                    e.printStackTrace();
                } catch(Exception e){
                    e.printStackTrace();
                }
        }
    }

    //private ArrayList<String> jobStatNameOrder = null;

    private final JSONArray jobJSONs = new JSONArray();


    //pointcut graphBoltExit(GraphStreamHandler gsh):
    pointcut graphBoltCleanup(GraphStreamHandler gsh):
           target(gsh) && call(void GraphStreamHandler.cleanup());




    after(GraphStreamHandler gsh) returning: graphBoltCleanup(gsh) {
        final boolean savingFlinkJobOperatorJSON = GraphStreamHandler.getInstance().savingFlinkJobOperatorJSON();

        if(savingFlinkJobOperatorJSON) {
            final String statisticsDirectory = GraphStreamHandler.getInstance().getStatisticsDirectory();
            //final Long iteration = GraphStreamHandler.getInstance().getIteration();
            final String operatorsStatsFileName = String.format(statisticsDirectory + "/flink_job_operator_json.json");
            final BufferedWriter out;
            try {
                out = new BufferedWriter(new FileWriter(operatorsStatsFileName));
                out.write(this.jobJSONs.toString(2));
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Save the Flink job operator statistics after executing.
     */
    after(ExecutionEnvironment env) returning: flinkJobExecuteCall(env) {



        final boolean isFlinkWebUIAvailable =
                GraphStreamHandler.getInstance().isRunningRemote();// ||
                //GraphStreamHandler.getInstance().runningLocalFlinkWebUI();


        if(!isFlinkWebUIAvailable) {
            return;
        }

        //final boolean savingFlinkJobOperatorStatistics = GraphStreamHandler.getInstance().savingFlinkJobOperatorStatistics();
        final boolean savingFlinkJobOperatorJSON = GraphStreamHandler.getInstance().savingFlinkJobOperatorJSON();
        //System.out.println("Saving Flink Job operator statistics:\t" + saveFlinkJobOperatorStatistics);

        //System.out.println("Is Flink W:\t" + isFlinkWebUIAvailable);

        try {

            final String address = GraphStreamHandler.getInstance().getFlinkJobManagerAddress();
            final String port = GraphStreamHandler.getInstance().getFlinkJobManagerPort();
            final JobExecutionResult jer = env.getLastJobExecutionResult();
            final JobID jid = jer.getJobID();
            final URL flinkRestURL = new URL("http://" + address + ":" + port + "/jobs/" + jid.toString());
            final URLConnection flinkRestConnection = flinkRestURL.openConnection();
            //final String statisticsDirectory = GraphStreamHandler.getInstance().getStatisticsDirectory();
            final Long iteration = GraphStreamHandler.getInstance().getIteration();
            final StringBuilder sb = new StringBuilder();

            String inputLine;
            final BufferedReader in = new BufferedReader(new InputStreamReader(flinkRestConnection.getInputStream()));
            while ((inputLine = in.readLine()) != null) {
                sb.append(inputLine);
            }
            in.close();

            final JSONObject json = new JSONObject(sb.toString());

            json.put(GraphStreamHandler.StatisticKeys.EXECUTION_COUNTER.toString(), iteration);

            if(savingFlinkJobOperatorJSON) {

                jobJSONs.put(json);

                /*
                final String operatorsStatsFileName = String.format(statisticsDirectory + "/job-%d_%s.json", iteration, jid.toString());
                final BufferedWriter out = new BufferedWriter(new FileWriter(operatorsStatsFileName));
                out.write(json.toString(2));
                out.close();
                */
            }


            /*
            if(savingFlinkJobOperatorStatistics && iteration > 0) {

                // Create the Flink job statistics directory if it doesn't exist yet.
                if(this.jobStatsPrintStream == null) {

                    final java.nio.file.Path dirs = Files.createDirectories(Paths.get(statisticsDirectory));

                    java.nio.file.Path file = dirs.resolve("flink_job_stats.tsv"); //placed in the statistics directory
                    if (!Files.exists(file)) {
                        file = Files.createFile(file);
                    }
                    this.jobStatsPrintStream = new PrintStream(file.toString());
                }

                if(this.jobStatNameOrder == null) {
                    this.jobStatNameOrder = new ArrayList<>();
                }

                final JSONArray vertices = json.getJSONArray("vertices");

                final HashMap<String, ArrayList<Long>> jobStats = new HashMap<>();
                jobStats.put("duration", new ArrayList<>());
                jobStats.put("parallelism", new ArrayList<>());
                jobStats.put("start-time", new ArrayList<>());
                jobStats.put("end-time", new ArrayList<>());
                //jobStats.put("name", new ArrayList<>());

                this.jobStatNameOrder.add();

                for (int i = 0, size = vertices.length(); i < size; i++) {

                    final JSONObject objectInArray = vertices.getJSONObject(i);

                    // "...and get thier component and thier value."
                    final String[] elementNames = JSONObject.getNames(objectInArray);
                    System.out.printf("%d ELEMENTS IN CURRENT OBJECT:\n", elementNames.length);
                    for (String elementName : elementNames)
                    {
                        final String value = objectInArray.getString(elementName);
                        System.out.printf("name=%s, value=%s\n", elementName, value);
                    }
                    System.out.println();

                }

            }

            */
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
