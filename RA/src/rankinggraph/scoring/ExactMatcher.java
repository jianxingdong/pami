package rankinggraph.scoring;

import java.util.List;

import rankinggraph.QueryInfo;
import rankinggraph.labelpropagation.MatchingRecord;
import rankinggraph.patterngeneration.TagsCombinationGenerator;

public class ExactMatcher implements PatternQueryMatcher {

	/**
	 * 
	 * @param pattern
	 * @param query
	 * @return 1 if matches 0 if not
	 */
	@Override
	public MatchingRecord getMatchScore(List<String> patternTokens,
			QueryInfo query) {
		if (patternTokens.size() != query.getNumTerms()) {
			return new MatchingRecord(0, null);
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
				return new MatchingRecord(0, null);

			}
		}
		// TODO find links
		return new MatchingRecord(1, null);
	}

}
