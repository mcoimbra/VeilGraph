package pt.ulisboa.tecnico.veilgraph.algorithm.pagerank;

import java.util.*;
import java.util.function.Function;

public class PageRankMain
{
    public static void main(final String[] args)
    {

		// Parse the options.
    	final PageRankParameterHelper ph = new PageRankParameterHelper(args);
		final Map<String, Object> argValues = ph.getParameters();

    	try {

        	// Configure the stream of updates.
            final Function<String, Long> vertexIdParser = (s -> Long.valueOf(s));
        	final PageRankStreamHandler graphBoltPageRank = new PageRankStreamHandler(argValues, vertexIdParser);

			graphBoltPageRank.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
