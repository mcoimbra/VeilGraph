package pt.ulisboa.tecnico.graph.output;

import org.apache.flink.api.common.io.OutputFormat;

/**
 * An output format with support for special tags and counters, suitable for iterative and continuous processing of graphs
 *
 * @param <T>
 * @author Renato Rosa
 */
public interface GraphOutputFormat<T> extends OutputFormat<T> {
    void setName(final String name);

    void setIteration(final Long iteration);

    void setTags(String... tags);
}
