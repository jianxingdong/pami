package rankinggraph.patterngeneration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import rankinggraph.QueryInfo;

/**
 * 
 * Query Terms are replaced with their corresponding named entities (if exist).
 * Short patterns of N-consecutive terms are generated.
 * 
 */
public class NGramPatternGenerator extends AbstractPatternGenerator {

	private final static int MIN_SIZE = 5;

	public NGramPatternGenerator(
			PatternGenerationNotifiable generationNotifiable) {
		super(generationNotifiable);
	}

	@Override
	public void generatePatterns(QueryInfo queryInfo) {
		int numTerms = queryInfo.getNumTerms();
		String[] fullPattern = queryInfo.getQueryTerms();
		HashMap<Integer, String> namedEntities = queryInfo.getNamedEntities();
		Set<Integer> keys = namedEntities.keySet();
		for (int k : keys) {
			fullPattern[k] = NE_PREFIX + namedEntities.get(k);
		}

		for (int numGrams = MIN_SIZE; numGrams <= numTerms; numGrams++) {
			for (int from = 0, to = numGrams - 1; to < numTerms; from++, to++) {
				generationNotifiable.notifyPattern(Arrays.copyOfRange(
						fullPattern, from, to));
			}
		}
	}
}