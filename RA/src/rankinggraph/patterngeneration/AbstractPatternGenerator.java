package rankinggraph.patterngeneration;

import rankinggraph.QueryInfo;

public abstract class AbstractPatternGenerator {
	public final static String NE_PREFIX = "ne-", POS_PREFIX = "pos-";

	protected PatternGenerationNotifiable generationNotifiable;

	public AbstractPatternGenerator(
			PatternGenerationNotifiable generationNotifiable) {
		this.generationNotifiable = generationNotifiable;
	}

	public abstract void generatePatterns(QueryInfo queryInfo);
}
