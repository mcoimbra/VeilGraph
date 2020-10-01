package pt.ulisboa.tecnico.veilgraph.output;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;

/**
 * A dummy {@link GraphOutputFormat} that discards everything
 *
 * @param <T>
 * @author Renato Rosa
 */
public class DiscardingGraphOutputFormat<T> implements GraphOutputFormat<T> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 7488712214278181208L;

	@Override
    public void setName(final String name) {

    }

    @Override
    public void setIteration(final Long iteration) {

    }

    @Override
    public void setTags(String... tags) {

    }

    @Override
    public void configure(final Configuration parameters) {

    }

    @Override
    public void open(final int taskNumber, final int numTasks) throws IOException {

    }

    @Override
    public void writeRecord(final T record) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
