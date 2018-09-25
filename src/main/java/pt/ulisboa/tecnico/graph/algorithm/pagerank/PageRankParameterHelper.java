package pt.ulisboa.tecnico.graph.algorithm.pagerank;


import org.apache.commons.cli.Option;

import pt.ulisboa.tecnico.graph.core.ParameterHelper;
import pt.ulisboa.tecnico.graph.model.randomwalk.BigVertexParameterHelper;


public class PageRankParameterHelper extends ParameterHelper {
	
	
	public enum PageRankArgumentName {

	    PAGERANK_ITERATIONS_SHORT("iter"), PAGERANK_ITERATIONS("iterations"), 
		PAGERANK_SIZE_SHORT("s"), PAGERANK_SIZE("size"), 
		PAGERANK_PERCENTAGE("percentage"),
	    DAMPENING_FACTOR_SHORT("damp"), DAMPENING_FACTOR("dampening");
		
		private final String text;
	    PageRankArgumentName(final String text) {
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

	@Override
	protected void setupCLIOptions() {
		super.setupCLIOptions();
		this.bvph.setupCLIOptions(super.options);

		// PageRank-specific parameters. 
		final Option iterationsOption = new Option(PageRankArgumentName.PAGERANK_ITERATIONS_SHORT.toString(), PageRankArgumentName.PAGERANK_ITERATIONS.toString(),
				true, "execute PageRank power method for a number of iterations.");
		iterationsOption.setRequired(false);
		super.options.addOption(iterationsOption);
		
		final Option rankOutputSizeOption = new Option(PageRankArgumentName.PAGERANK_SIZE_SHORT.toString(), PageRankArgumentName.PAGERANK_SIZE.toString(),
				true, "number of rank elements produced. Default to 1000 if not provided. Accepts a percentage as well: '-size 10%'.");
		rankOutputSizeOption.setRequired(false);
		super.options.addOption(rankOutputSizeOption);

		final Option rankPercentageOption = new Option(PageRankArgumentName.PAGERANK_PERCENTAGE.toString(), PageRankArgumentName.PAGERANK_SIZE.toString(),
				true, "percentage of rank elements produced. Default to -1%.");
				rankPercentageOption.setRequired(false);
		super.options.addOption(rankPercentageOption);



		final Option pageRankDampeningFactorOption = new Option(PageRankArgumentName.DAMPENING_FACTOR_SHORT.toString(), PageRankArgumentName.DAMPENING_FACTOR.toString(), true, "pagerank dampening factor (default 0.85).");
		pageRankDampeningFactorOption.setRequired(false);
		super.options.addOption(pageRankDampeningFactorOption);
	}


	private BigVertexParameterHelper bvph = new BigVertexParameterHelper();

	@Override
	protected void parseValues(final String[] args) {
		super.parseValues(args);
		this.bvph.parseValues(args, super.argValues, super.cmd);
		
		// Default to PageRank power method of 30 iterations if it is not provided.
		if(super.cmd.hasOption(PageRankArgumentName.PAGERANK_ITERATIONS.toString())) {
			final Integer iterations = Integer.parseInt(super.cmd.getOptionValue(PageRankArgumentName.PAGERANK_ITERATIONS.toString()));

			if(iterations <= 0)
				throw new IllegalArgumentException(PageRankArgumentName.PAGERANK_ITERATIONS.toString() + " must be a positive integer.");

			super.argValues.put(PageRankArgumentName.PAGERANK_ITERATIONS.toString(), iterations);
		}
		else {
			super.argValues.put(PageRankArgumentName.PAGERANK_ITERATIONS.toString(), new Integer(30));
		}

		// Default to a PageRank result size of 1000 vertices if it is not provided.
		if(super.cmd.hasOption(PageRankArgumentName.PAGERANK_SIZE.toString())) {
			final Integer size = Integer.parseInt(super.cmd.getOptionValue(PageRankArgumentName.PAGERANK_SIZE.toString()));

			if(size <= 0)
				throw new IllegalArgumentException(PageRankArgumentName.PAGERANK_SIZE.toString() + " must be a positive integer.");

			super.argValues.put(PageRankArgumentName.PAGERANK_SIZE.toString(), size);
		}
		else {
			super.argValues.put(PageRankArgumentName.PAGERANK_SIZE.toString(), new Integer(1000));
		}

		if(super.cmd.hasOption(PageRankArgumentName.PAGERANK_PERCENTAGE.toString())) {
			final Integer percentage = Integer.parseInt(super.cmd.getOptionValue(PageRankArgumentName.PAGERANK_PERCENTAGE.toString()));

			if(percentage <= 0)
				throw new IllegalArgumentException(PageRankArgumentName.PAGERANK_PERCENTAGE.toString() + " must be a positive integer.");

			super.argValues.put(PageRankArgumentName.PAGERANK_PERCENTAGE.toString(), percentage);
		}
		else {

		}

		// Default to PageRank dampening factor of 0.85.
		if(super.cmd.hasOption(PageRankArgumentName.DAMPENING_FACTOR.toString())) {
			final Double dampening = Double.parseDouble(super.cmd.getOptionValue(PageRankArgumentName.DAMPENING_FACTOR.toString()));

			if(dampening <= 0.000 || dampening > 1.000)
				throw new IllegalArgumentException(PageRankArgumentName.DAMPENING_FACTOR.toString() + " must be a positive double in [0; 1[.");

			super.argValues.put(PageRankArgumentName.DAMPENING_FACTOR.toString(), dampening);
		}
		else {
			super.argValues.put(PageRankArgumentName.DAMPENING_FACTOR.toString(), new Double(0.850000000d));
		}
	}
	
	public PageRankParameterHelper(final String[] args) {
		this.bvph = new BigVertexParameterHelper();
		this.setupCLIOptions();
		this.parseValues(args);
	}

	/**
	 * We want our output base name to be composed of the passed PageRank parameters.
	 * @return
	 */
	@Override
	public String getFileSuffix() {
		return this.bvph.getFileSuffix();
	}
}
