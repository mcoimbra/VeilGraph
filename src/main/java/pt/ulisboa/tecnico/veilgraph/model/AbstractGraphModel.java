package pt.ulisboa.tecnico.veilgraph.model;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringJoiner;

public abstract class AbstractGraphModel<K, VV, EV, R> implements GraphModel<K, VV, EV, R>, Serializable {

    private static final long serialVersionUID = -219967429214937736L;

    protected HashMap<String, ArrayList<Long>> statisticsMap = new HashMap<>();

    protected Long iteration;
    protected Boolean dumpingModel = false;

    protected String statisticsDirectory;
    protected String modelDirectory;
    protected PrintStream printStream = null;


    public HashMap<String, ArrayList<Long>> getStatisticsMap() {
        return this.statisticsMap;
    }

    @Override
    public void setModelDirectory(final String modelDirectory) {
        this.modelDirectory = modelDirectory;
    }

    @Override
    public void cleanup() {
        this.printStream.close();
    }


    @Override
    public void initStatistics(final String statisticsDirectory) {
        this.statisticsDirectory = statisticsDirectory;

        // Create file stream to which model statistics will be written.
        final java.nio.file.Path dirs;
        try {
            dirs = Files.createDirectories(Paths.get(this.statisticsDirectory));

            java.nio.file.Path file = dirs.resolve(this.toString() + ".tsv");
            if (!Files.exists(file)) {
                file = Files.createFile(file);
            }
            this.printStream = new PrintStream(file.toString());

            // Initialize model statistic names.
            final StringJoiner statJoiner = new StringJoiner(";");
            statJoiner.add("execution_count");
            for(final String statName: this.statisticsMap.keySet()) {
                if( ! statName.equals("execution_count"))
                    statJoiner.add(statName);
            }
            final String statLine = statJoiner.toString();
            this.printStream.println(statLine);
            this.printStream.flush();


        } catch (IOException e) {
            e.printStackTrace();
        }
    };


    @Override
    public abstract String toString();

}
