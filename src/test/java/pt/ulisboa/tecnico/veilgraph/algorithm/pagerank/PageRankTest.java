package pt.ulisboa.tecnico.veilgraph.algorithm.pagerank;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.BeforeAll;
import pt.ulisboa.tecnico.veilgraph.pt.ulisboa.tecnico.graph.AbstractGraphTest;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class PageRankTest extends AbstractGraphTest {

    /**
     * Get the results for each computed PageRank step so far.
     * @return the list of results.
     */
    public ArrayList<DataSet<Tuple2<Long, Double>>> getOfflineResults() {
        return this.offlineResults;
    }
    private final ArrayList<DataSet<Tuple2<Long, Double>>> offlineResults = new ArrayList<DataSet<Tuple2<Long, Double>>>();

    /**

    /**
     * Private constructor for the singleton pattern.
     */
    private PageRankTest() {}

    /**
     * Singleton design pattern instance access.
     * @return the singleton instance.
     */
    protected static PageRankTest getInstance() {
        if(singleton == null)
            singleton = new PageRankTest();
        return (PageRankTest) singleton;
    }

    @BeforeAll
    protected static void initAll() {
        final PageRankTest instance = PageRankTest.getInstance();
        instance.setup();

        try {

            final String step0RankPath = new File(AbstractGraphTest.class.getResource("/step_0_python_powermethod_pr.tsv").toURI()).getPath();
            final DataSet<Tuple2<Long, Double>> ranks0 = instance.getContext()
                    .readCsvFile(step0RankPath)
                    .ignoreComments("#")
                    .fieldDelimiter("\t")
                    .types(Long.class, Double.class)
                    .sortPartition(1, Order.DESCENDING);
            instance.getOfflineResults().add(ranks0);

            final String step1RankPath = new File(AbstractGraphTest.class.getResource("/step_1_python_powermethod_pr.tsv").toURI()).getPath();
            final DataSet<Tuple2<Long, Double>> ranks1 = instance.getContext()
                    .readCsvFile(step1RankPath)
                    .ignoreComments("#")
                    .fieldDelimiter("\t")
                    .types(Long.class, Double.class)
                    .sortPartition(1, Order.DESCENDING);

            instance.getOfflineResults().add(ranks1);
        }
        catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
