package pt.ulisboa.tecnico.graph.core;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.json.JSONObject;
import org.json.JSONArray;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

import pt.ulisboa.tecnico.graph.stream.GraphStreamHandler;

/**
 * Helpful information on AspectJ.
 *
 * https://www.eclipse.org/aspectj/doc/released/
 * https://www.eclipse.org/aspectj/doc/released/progguide/starting-aspectj.html#the-dynamic-join-point-model
 * https://www.eclipse.org/aspectj/doc/released/quick.pdf
*/

public aspect ExecutionEnvironmentAspect {

    /**
     * Detect calls to Flink's {@link org.apache.flink.api.java.ExecutionEnvironment}.
     */
    pointcut flinkJobExecuteCall(ExecutionEnvironment env):
            target(env) &&
            (call(JobExecutionResult ExecutionEnvironment.execute()) || call(JobExecutionResult ExecutionEnvironment.execute(String)));

    /**
     * Write the Flink execution plan before actually calling {@link org.apache.flink.api.java.ExecutionEnvironment}.
     */
    before(ExecutionEnvironment env): flinkJobExecuteCall(env) {

        final boolean savingFlinkPlans = GraphStreamHandler.getInstance().savingFlinkPlans();
        if (savingFlinkPlans) {
            try {
                    final String plansDirectory = GraphStreamHandler.getInstance().getPlansDirectory();
                    final Long iteration = GraphStreamHandler.getInstance().getIteration();

                    final String planJSON = env.getExecutionPlan();

                    //TODO: instead of a hash, the plan name could be something more human-friendly...
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

    private final JSONArray jobJSONs = new JSONArray();

    /**
     * Detect the GraphBolt call to {@link pt.ulisboa.tecnico.graph.stream.GraphStreamHandler}.
     */
    pointcut graphBoltCleanup(GraphStreamHandler gsh):
           target(gsh) && call(void GraphStreamHandler.cleanup());

    /**
     * Write the Flink job operator statistics to a JSON file.
     */
    after(GraphStreamHandler gsh) returning: graphBoltCleanup(gsh) {
        final boolean savingFlinkJobOperatorJSON = GraphStreamHandler.getInstance().savingFlinkJobOperatorJSON();

        if(savingFlinkJobOperatorJSON) {
            final String statisticsDirectory = GraphStreamHandler.getInstance().getStatisticsDirectory();
            final String operatorsStatsFileName = String.format(statisticsDirectory + "/flink_job_operators.json");
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
                GraphStreamHandler.getInstance().isRunningRemote() ||
                GraphStreamHandler.getInstance().runningLocalFlinkWebUI();

        if(!isFlinkWebUIAvailable) {
            //TODO: this should be moved to a pointcut that executes before the RETURN of env.execute().
            return;
        }

        final boolean savingFlinkJobOperatorStatistics = GraphStreamHandler.getInstance().savingFlinkJobOperatorStatistics();
        final boolean savingFlinkJobOperatorJSON = GraphStreamHandler.getInstance().savingFlinkJobOperatorJSON();
        System.out.println("Saving Flink Job operator statistics:\t" + savingFlinkJobOperatorStatistics);

        //System.out.println("Is Flink W:\t" + isFlinkWebUIAvailable);

        try {

            final String address = GraphStreamHandler.getInstance().getFlinkJobManagerAddress();
            final String port = GraphStreamHandler.getInstance().getFlinkJobManagerPort();
            final JobExecutionResult jer = env.getLastJobExecutionResult();
            final JobID jid = jer.getJobID();
            final URL flinkRestURL = new URL("http://" + address + ":" + port + "/jobs/" + jid.toString());
            final URLConnection flinkRestConnection = flinkRestURL.openConnection();
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
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
