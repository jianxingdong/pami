package rankinggraph.scoring;

import java.util.List;

import parsers.Utils;
import rankinggraph.QueryInfo;
import rankinggraph.patterngeneration.QueryPatternsGenerator;

public class PatternQueryMatcher {

	/**
	 * 
	 * @param pattern
	 * @param query
	 * @return 1 if matches 0 if not
	 */
	public float getExactMatchScore(String pattern, QueryInfo query) {
		List<String> patternTokens = Utils.tokenize(pattern);
		if (patternTokens.size() != query.getNumTerms()) {
			return 0;
		}
		String queryToken = null, patternToken = null;
		int numTokens = patternTokens.size();
		for (int i = 0; i < numTokens; i++) {
			if (patternTokens.get(i).startsWith(
					QueryPatternsGenerator.POS_PREFIX)) {
				queryToken = query.getPartOfSpeeches()[i];
				patternToken = patternTokens.get(i).substring(4);
			} else if (patternTokens.get(i).startsWith(
					QueryPatternsGenerator.NE_PREFIX)) {
				queryToken = query.getNamedEntities().get(i);
				patternToken = patternTokens.get(i).substring(3);
			} else {
				queryToken = query.getQueryTerms()[i];
				patternToken = patternTokens.get(i);
			}

			if (!patternToken.equalsIgnoreCase(queryToken)) {
				return 0;
			}
		}
		return 1;
	}

	public float getEditDistanceScore(String pattern, QueryInfo query) {
		// TODO edit distance, lower-case, normalize by query length
		return 0;
	}

	public static void main(String[] args) {
		

		System.out.println(Utils.glueTokens("All flights from ne-LOC to San-Francisco on ne-ORG first class please".split("\\s")));
	}
}
