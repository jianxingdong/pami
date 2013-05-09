package rankinggraph.patterngeneration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import rankinggraph.PatternSupport;
import rankinggraph.QueryInfo;
import rankinggraph.scoring.PatternMatchNotifiable;
import rankinggraph.scoring.PatternQueryMatcher;

public class PrunedNGramGenerator extends AbstractPatternGenerator {

	private PatternSupport patternSupport;
	private final static int MIN_SIZE = 5;

	public PrunedNGramGenerator(
			PatternGenerationNotifiable generationNotifiable,
			String parsedQueriesFile,
			PatternMatchNotifiable patternMatchNotifiable,
			PatternQueryMatcher matcher) throws Exception {
		super(generationNotifiable);
		patternSupport = new PatternSupport(matcher, parsedQueriesFile,
				patternMatchNotifiable);
	}

	@Override
	public void generatePatterns(QueryInfo queryInfo) {
		int numTerms = queryInfo.getNumTerms();
		String[] fullPattern = queryInfo.getQueryTerms();
		HashMap<Integer, String> namedEntities = queryInfo.getNamedEntities();
		Set<Integer> keys = namedEntities.keySet();
		// no patterns are generated if no named entities
		if (keys.isEmpty()) {
			return;
		}
		for (int k : keys) {
			fullPattern[k] = NE_PREFIX + namedEntities.get(k);
		}
		int patternId = generationNotifiable.notifyPattern(fullPattern);
		/*
		 * BitSet support = patternSupport.getSupport(patternId,
		 * glueTokens(fullPattern)); if (support.isEmpty()) { throw new
		 * RuntimeException("Empty: " + glueTokens(fullPattern)); }
		 * notifyMatches(patternId, support);
		 */
		patternSupport.match(patternId, Arrays.asList(fullPattern));

		for (int numGrams = MIN_SIZE; numGrams < numTerms; numGrams++) {
			for (int from = 0, to = numGrams - 1; to < numTerms; from++, to++) {
				String[] subPattern = getSubPattern(fullPattern, from, to);
				if (subPattern != null) {
					patternId = generationNotifiable.notifyPattern(subPattern);
					patternSupport.match(patternId, Arrays.asList(fullPattern));

					/*
					 * BitSet subPatternSupport = patternSupport.getSupport(
					 * patternId, glueTokens(subPattern)); if
					 * (subPatternSupport.isEmpty()) { throw new
					 * RuntimeException("Empty: " + glueTokens(subPattern)); }
					 * if (true) { // TODO !support.equals(subPatternSupport)
					 * generationNotifiable.notifyPattern(subPattern);
					 * notifyMatches(patternId, subPatternSupport); patternId++;
					 * }
					 */
				}

			}
		}
	}

	/*
	 * private void notifyMatches(int patternId, BitSet support) { // TODO this
	 * is a hack int matchIndex = 0; for (int i = support.nextSetBit(0); i >= 0;
	 * i = support .nextSetBit(i + 1)) { patternMatchNotifiable.notifyMatch(i,
	 * patternId, this.patternSupport.matches.get(matchIndex++)); //
	 * this.patternSupport.scores.get(matchIndex++) }
	 * 
	 * }
	 */
	/**
	 * 
	 * @param pattern
	 * @param from
	 * @param to
	 * @return null if the sub-pattern to be generated does not contain any
	 *         named entities.
	 */
	private String[] getSubPattern(String[] pattern, int from, int to) {
		int len = to - from + 1;
		String[] subPattern = new String[len];
		boolean containsNE = false;
		for (int i = 0; i < len; i++) {
			subPattern[i] = pattern[from + i];
			containsNE = containsNE
					|| subPattern[i]
							.startsWith(AbstractPatternGenerator.NE_PREFIX);
		}
		if (containsNE)
			return subPattern;
		else
			return null;
	}
}