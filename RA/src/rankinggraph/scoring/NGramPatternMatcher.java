package rankinggraph.scoring;

import java.util.List;

import parsers.Utils;
import rankinggraph.QueryInfo;
import rankinggraph.QueryParser;
import rankinggraph.patterngeneration.AbstractPatternGenerator;
import rankinggraph.patterngeneration.TagsCombinationGenerator;

public class NGramPatternMatcher implements PatternQueryMatcher {

	/**
	 * return 1 if the pattern matches a substring of the query. 0 otherwise
	 */
	@Override
	public float getMatchScore(String pattern, QueryInfo query) {
		List<String> patternTokens = Utils.tokenize(pattern);
		int numPatternTerms = patternTokens.size();
		if (numPatternTerms == 0)
			return 0;
		String firstToken = patternTokens.get(0);
		int numQueryTerms = query.getNumTerms();
		int startIndex = -1;
		int startMatchFrom = 0;
		boolean isFirstNE = firstToken
				.startsWith(AbstractPatternGenerator.NE_PREFIX);
		if (isFirstNE) {
			firstToken = firstToken
					.substring(AbstractPatternGenerator.NE_PREFIX.length());
		}
		do {
			startIndex = -1;
			if (isFirstNE) {
				startIndex = query.getNamedEntityIndex(firstToken,
						startMatchFrom);
			} else {
				startIndex = Utils.find(query.getQueryTerms(), firstToken,
						startMatchFrom);
			}
			if (startIndex == -1
					|| numQueryTerms - startIndex < numPatternTerms) {
				return 0;
			}
			// match
			if (matches(patternTokens, query, startIndex + 1)) {
				return 1;
			}

			startMatchFrom = startIndex + 1;
		} while (startMatchFrom < numQueryTerms);
		return 0;
	}

	/**
	 * first pattern token is skipped
	 * 
	 * @param patternTokens
	 * @param query
	 * @param startIndex
	 * @return
	 */
	private boolean matches(List<String> patternTokens, QueryInfo query,
			int startIndex) {
		int numTokens = patternTokens.size();
		String queryToken = null, patternToken = null;
		for (int i = 1; i < numTokens; i++) {
			if (patternTokens.get(i).startsWith(
					TagsCombinationGenerator.NE_PREFIX)) {
				queryToken = query.getNamedEntities().get(i + startIndex - 1);
				patternToken = patternTokens.get(i).substring(
						TagsCombinationGenerator.NE_PREFIX.length());
			} else {
				queryToken = query.getQueryTerms()[i + startIndex - 1];
				patternToken = patternTokens.get(i);
			}
			if (!patternToken.equals(queryToken)) {
				
				return false;
			}
		}
		return true;
	}

	public static void main(String[] args) {
		String pattern = "from ne-LOC to ne-LOC on ne-ORG";
		String q = "All flight from Atlanta to San Francisco on Delta first class please.", 
		pos = "DT NNS IN NNP TO NNP NNP IN NNP JJ NN VB .",
		ne = "O O O LOC-B O LOC-B LOC-I O ORG-B O O O O";
		QueryInfo query = new QueryParser().parse(q, pos, ne);
		System.out.println(new NGramPatternMatcher().getMatchScore(pattern,
				query));
	}
}
