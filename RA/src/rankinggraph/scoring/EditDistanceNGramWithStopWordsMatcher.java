package rankinggraph.scoring;

import java.util.List;

import parsers.StopWords;
import parsers.Utils;
import rankinggraph.QueryInfo;
import rankinggraph.patterngeneration.AbstractPatternGenerator;

public class EditDistanceNGramWithStopWordsMatcher implements
		PatternQueryMatcher {

	public static final float STOP_WORD_SCORE = 0.5f;

	@Override
	public MatchingRecord getMatchScore(String pattern, QueryInfo query) {

		List<String> patternTokens = Utils.tokenize(pattern);
		int patternLength = patternTokens.size();
		if (patternLength == 0)
			return new MatchingRecord(0, null);
		MatchingRecord match = computeLevenshteinDistance(patternTokens, query);
		float editDistance = match.getMatchingScore();
		float minLen = Math.min(patternLength, query.getNumTerms());

		if (editDistance >= minLen)
			return new MatchingRecord(0, null);
		return new MatchingRecord(1 - editDistance / minLen, match.getLinks());
	}

	private MatchingRecord computeLevenshteinDistance(
			List<String> patternTokens, QueryInfo query) {
		int queryLen = query.getNumTerms();
		int patternLen = patternTokens.size();
		float[][] distance = new float[queryLen + 1][patternLen + 1];

		int[][] path = new int[queryLen][patternLen];

		for (int j = 1; j <= patternLen; j++)
			distance[0][j] = j;

		for (int i = 1; i <= queryLen; i++) {

			for (int j = 1; j <= patternLen; j++) {
				boolean ptrnTknStopWord = StopWords.contains(patternTokens
						.get(j - 1));
				boolean qrtTknStopWord = StopWords.contains(query
						.getQueryTerms()[i - 1]);
				boolean tknsMatch = matches(patternTokens.get(j - 1), query,
						i - 1);
				ArrayEntry minEntry = minimum(
						distance[i - 1][j]
								+ (qrtTknStopWord ? STOP_WORD_SCORE : 1),
						distance[i][j - 1]
								+ (ptrnTknStopWord ? STOP_WORD_SCORE : 1),
						distance[i - 1][j - 1]
								+ (tknsMatch ? 0
										: (ptrnTknStopWord || qrtTknStopWord) ? STOP_WORD_SCORE
												: 1));
				distance[i][j] = minEntry.value;
				// 0 -> delete, 1-> insert, 2-> substitute, 3-> exact
				path[i - 1][j - 1] = tknsMatch && minEntry.index == 2 ? 3
						: minEntry.index;
			}

		}
		float min = Float.MAX_VALUE;
		int indx = -1;
		for (int i = 1; i <= queryLen; i++) {
			if (distance[i][patternLen] < min) {
				min = distance[i][patternLen];
				indx = i;
			}
		}

		// backtracking, searching for the exact matches in the path of the best
		// matching.
		PatternQueryLinks links = new PatternQueryLinks(patternLen, queryLen);
		int i = indx - 1, j = patternLen - 1;

		while (i >= 0 && j >= 0) {
			switch (path[i][j]) {
			case 0:
				i--;
				break;
			case 1:
				j--;
				break;
			case 3:
				// exact match
				links.addLink(i, j);
			case 2:
				i--;
				j--;
				break;
			}
		}
		return new MatchingRecord(min, links);
	}

	private boolean matches(String patternToken, QueryInfo query,
			int queryTermIndex) {
		String queryToken;
		if (patternToken.startsWith(AbstractPatternGenerator.NE_PREFIX)) {
			queryToken = query.getNamedEntities().get(queryTermIndex);
			patternToken = patternToken
					.substring(AbstractPatternGenerator.NE_PREFIX.length());
		} else {
			queryToken = query.getQueryTerms()[queryTermIndex];
		}
		return patternToken.equalsIgnoreCase(queryToken);
	}

	private static class ArrayEntry {
		float value;
		int index;

		ArrayEntry(float value, int index) {
			this.value = value;
			this.index = index;
		}

	}

	private ArrayEntry minimum(float... vals) {
		float min = Float.MAX_VALUE;
		int minIndx = -1;
		for (int i = 0; i < vals.length; i++) {
			if (vals[i] < min) {
				min = vals[i];
				minIndx = i;
			}
		}
		return new ArrayEntry(min, minIndx);
	}

}