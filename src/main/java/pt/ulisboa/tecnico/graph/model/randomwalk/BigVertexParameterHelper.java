package pt.ulisboa.tecnico.graph.model.randomwalk;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import pt.ulisboa.tecnico.graph.algorithm.pagerank.PageRankParameterHelper;
import pt.ulisboa.tecnico.graph.core.ParameterHelper;

import java.util.Map;

public class BigVertexParameterHelper extends ParameterHelper {

    public enum BigVertexArgumentName {


        RATIO_PARAM_SHORT("r"), RATIO_PARAM("ratio"),
        NEIGHBORHOOD_PARAM_SHORT("n"), NEIGHBORHOOD_PARAM("neighborhood"),
        DELTA_PARAM_SHORT("d"), DELTA_PARAM("delta");


        private final String text;
        BigVertexArgumentName(final String text) {
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


    public void setupCLIOptions(final Options options) {
        final Option thresholdOption = new Option(BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM_SHORT.toString(), BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM.toString(),
                true, "parameter r for minimum degree of vertex change.");
        thresholdOption.setRequired(false);
        options.addOption(thresholdOption);

        final Option neighborhoodOption = new Option(BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM_SHORT.toString(), BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM.toString(),
                true, "parameter n for summarization vertex neighborhood radius.");
        neighborhoodOption.setRequired(false);
        options.addOption(neighborhoodOption);

        final Option deltaOption = new Option(BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM_SHORT.toString(), BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM.toString(),
                true, "parameter delta for per-vertex neighborhood expansion.");
        deltaOption.setRequired(false);
        options.addOption(deltaOption);
    }



    public void parseValues(final String[] args, final Map<String, Object> argValues, final CommandLine cmd) {
        if(	(! cmd.hasOption(BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM.toString())) ||
                (! cmd.hasOption(BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM.toString())) ||
                (! cmd.hasOption(BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM.toString()))) {

            // We are going to compute the exact (non-summarized) version of PageRank.
            System.out.println("Running the non-summarized version of PageRank.");
        }
        else {
            System.out.println("Running the summarized version of PageRank.");


            final Double r = Double.parseDouble(cmd.getOptionValue(BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM.toString()));
            if(r < 0.000d)
                throw new IllegalArgumentException(BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM.toString() + " must be a non-negative double.");

            final Integer n = Integer.parseInt(cmd.getOptionValue(BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM.toString()));
            if(n < 0)
                throw new IllegalArgumentException(BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM.toString() + " must be a non-negative integer.");

            final Double delta = Double.parseDouble(cmd.getOptionValue(BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM.toString()));
            if(delta < 0.000d)
                throw new IllegalArgumentException(BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM.toString() + " must be a non-negative double.");

            argValues.put(BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM.toString(), r);
            argValues.put(BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM.toString(), n);
            argValues.put(BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM.toString(), delta);

            // Check if intermediate summary graphs are to be used.
            final Boolean dumpingSummaries = cmd.hasOption(GraphBoltArgumentName.DUMP_MODEL.toString());
            argValues.put(GraphBoltArgumentName.DUMP_MODEL.toString(), dumpingSummaries);
        }
    }

    @Override
    public String getFileSuffix() {

        if(argValues.containsKey(BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM.toString()) &&
                argValues.containsKey(BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM.toString()) &&
                argValues.containsKey(BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM.toString())) {
            final Double threshold = (Double) argValues.get(BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM.toString());
            final Integer neighborhoodSize = (Integer) argValues.get(BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM.toString());
            final Double delta = (Double) argValues.get(BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM.toString());

            return String.format("%02.2f_%d_%02.3f", threshold, neighborhoodSize, delta).replace(',', '.');
        }

        return "";
    }
}
