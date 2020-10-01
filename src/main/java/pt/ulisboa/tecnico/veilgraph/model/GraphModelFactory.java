package pt.ulisboa.tecnico.veilgraph.model;

import pt.ulisboa.tecnico.veilgraph.model.randomwalk.BigVertexGraph;
import pt.ulisboa.tecnico.veilgraph.model.randomwalk.BigVertexParameterHelper;

import java.util.Map;

public class GraphModelFactory<VV, EV> {

    public GraphModel getGraphModel(final Model graphModelType, final Map<String, Object> argValues, String modelsDirectory) {

        GraphModel model;

        if(graphModelType == null) {
            throw new IllegalArgumentException("Graph model type can't be null.");
        }
        else if(graphModelType == Model.BIG_VERTEX) {

            final Double updateRatioThreshold = (Double) argValues.get(BigVertexParameterHelper.BigVertexArgumentName.RATIO_PARAM.toString());
            final Integer neighborhoodSize = (Integer) argValues.get(BigVertexParameterHelper.BigVertexArgumentName.NEIGHBORHOOD_PARAM.toString());
            final Double delta = (Double) argValues.get(BigVertexParameterHelper.BigVertexArgumentName.DELTA_PARAM.toString());

            model = new BigVertexGraph<VV, EV>(updateRatioThreshold, neighborhoodSize, delta);
        }
        else {
            throw new IllegalArgumentException("Unknown graph model:\t" + graphModelType);
        }

        model.setModelDirectory(modelsDirectory);

        return model;
    }

    public enum Model {
        // All undertaken strategies since execution began.
        BIG_VERTEX("BIG_VERTEX");




        private final String text;

        /**
         * @param text
         */
        Model(final String text) {
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
}
