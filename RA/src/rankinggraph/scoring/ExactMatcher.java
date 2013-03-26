package rankinggraph.scoring;

import java.util.List;

import parsers.Utils;
import rankinggraph.QueryInfo;
import rankinggraph.patterngeneration.TagsCombinationGenerator;

public class ExactMatcher implements PatternQueryMatcher {

	/**
	 * 
	 * @param pattern
	 * @param query
	 * @return 1 if matches 0 if not
	 */
	@Override
	public float getMatchScore(String pattern, QueryInfo query) {
		List<String> patternTokens = Utils.tokenize(pattern);
		if (patternTokens.size() != query.getNumTerms()) {
			return 0;
		}
		String queryToken = null, patternToken = null;
		int numTokens = patternTokens.size();
		for (int i = 0; i < numTokens; i++) {
			if (patternTokens.get(i).startsWith(
					TagsCombinationGenerator.POS_PREFIX)) {
				queryToken = query.getPartOfSpeeches()[i];
				patternToken = patternTokens.get(i).substring(4);
			} else if (patternTokens.get(i).startsWith(
					TagsCombinationGenerator.NE_PREFIX)) {
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

}
