package rankinggraph.scoring;

import parsers.Utils;
import rankinggraph.QueryInfo;

public class PatternQueryMatcher {

	/**
	 * 
	 * @param pattern
	 * @param query
	 * @return 1 if matches 0 if not
	 */
	public float getExactMatchScore(String pattern, QueryInfo query) {
		String[] patternTokens = Utils.tokenize(pattern);
		if (patternTokens.length != query.getNumTerms()) {
			return 0;
		}
		String queryToken = null, patternToken = null;
		for (int i = 0; i < patternTokens.length; i++) {
			if (patternTokens[i].startsWith("pos-")) {
				queryToken = query.getPartOfSpeeches()[i];
				patternToken = patternTokens[i].substring(4);
			} else if (patternTokens[i].startsWith("ne-")) {
				queryToken = query.getNamedEntities().get(i);
				patternToken = patternTokens[i].substring(3);
			} else {
				queryToken = query.getQueryTerms()[i];
				patternToken = patternTokens[i];
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

}
