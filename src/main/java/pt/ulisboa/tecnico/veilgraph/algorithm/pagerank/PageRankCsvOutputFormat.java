package pt.ulisboa.tecnico.veilgraph.algorithm.pagerank;

import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import pt.ulisboa.tecnico.veilgraph.output.GraphOutputFormat;

import java.io.IOException;

/**
 * CSV output for PageRank
 *
 * @author Renato Rosa
 */
public class PageRankCsvOutputFormat<K> implements GraphOutputFormat<Tuple2<K, Double>> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 6266421069609723311L;
	private final Path outputDir;
    private final boolean printRanks;
    private String name;
    private Long iteration;
    private String[] tags;
    private CsvOutputFormat<? extends Tuple> format;

    public PageRankCsvOutputFormat(final String outputDir, final String recordDelimiter, final String fieldDelimiter, final boolean printRanks, final boolean overwrite) {
        this.outputDir = new Path(outputDir);
        this.printRanks = printRanks;
        if (printRanks) {
            this.format = new CsvOutputFormat<Tuple2<K, Double>>(new Path(), recordDelimiter, fieldDelimiter);
        } else {
        	this.format = new CsvOutputFormat<Tuple1<K>>(new Path(), recordDelimiter, fieldDelimiter);
        }
        this.format.setWriteMode(overwrite ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE);
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public void setIteration(final Long iteration) {
        this.iteration = iteration;
    }

    @Override
    public void setTags(String... tags) {
        this.tags = tags.clone();
    }

    @Override
    public void configure(Configuration parameters) {
    	this.format.configure(parameters);
    }

    @Override
    public void open(final int taskNumber, final int numTasks) throws IOException {
        String fileName = String.format("%s-%04d-%s.csv", name, iteration, (tags.length > 0 ? tags[0] : ""));
        this.format.setOutputFilePath(new Path(this.outputDir, fileName));
        this.format.open(taskNumber, numTasks);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeRecord(final Tuple2<K, Double> record) throws IOException {
        if (this.printRanks) {
            ((CsvOutputFormat<Tuple2<K, Double>>) this.format).writeRecord(record);
        } else {
            ((CsvOutputFormat<Tuple1<K>>) this.format).writeRecord(Tuple1.of(record.f0));
        }
    }

    @Override
    public void close() throws IOException {
    	this.format.close();
    }
}
