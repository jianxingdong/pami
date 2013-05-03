package rankinggraph.scoring;

import java.util.List;

import parsers.StopWords;
import parsers.Utils;
import rankinggraph.QueryInfo;
import rankinggraph.patterngeneration.AbstractPatternGenerator;

public class EditDistanceNGramWithStopWordsMatcher implements
		PatternQueryMatcher {

	public static final float STOP_WORD_SCORE = 0.5f;

	private final float SCORE_DIFF;
	{
		SCORE_DIFF = 1 - STOP_WORD_SCORE;
	}

	@Override
	public float getMatchScore(String pattern, QueryInfo query) {

		List<String> patternTokens = Utils.tokenize(pattern);
		int patternLength = patternTokens.size();
		if (patternLength == 0)
			return 0;
		float editDistance = computeLevenshteinDistance(patternTokens, query);
		float minLen = Math.min(patternLength, query.getNumTerms());

		if (editDistance >= minLen)
			return 0;
		return 1 - editDistance / minLen; // TODO
	}

	public float computeLevenshteinDistance(List<String> patternTokens,
			QueryInfo query) {
		int queryLen = query.getNumTerms();
		int patternLen = patternTokens.size();
		float[][] distance = new float[queryLen + 1][patternLen + 1];
		// int[][] path = new int[queryLen][patternLen];

		// for (int i = 0; i <= queryLen; i++)
		// distance[i][0] = i;
		for (int j = 1; j <= patternLen; j++)
			distance[0][j] = j;

		for (int i = 1; i <= queryLen; i++) {

			for (int j = 1; j <= patternLen; j++) {
				boolean ptrnTknStopWord = StopWords.contains(patternTokens
						.get(j - 1));
				boolean qrtTknStopWord = StopWords.contains(query
						.getQueryTerms()[i - 1]);
				distance[i][j] = minimum(
						distance[i - 1][j]
								+ (qrtTknStopWord ? STOP_WORD_SCORE : 1),
						distance[i][j - 1]
								+ (ptrnTknStopWord ? STOP_WORD_SCORE : 1),
						distance[i - 1][j - 1]
								+ (matches(patternTokens.get(j - 1), query,
										i - 1) ? 0
										: (ptrnTknStopWord || qrtTknStopWord) ? STOP_WORD_SCORE
												: 1));
			}

		}
		float min = Float.MAX_VALUE;
		// int indx = -1;
		for (int i = 1; i <= queryLen; i++) {
			if (distance[i][patternLen] < min) {
				min = distance[i][patternLen];
				// indx = i;
			}
		}
		/*
		 * // backtracking, searching for the first exact match in the matching
		 * // path int i = indx - 1, j = patternLen - 1; int exctI = 0, exctJ =
		 * 0; while (i >= 0 && j >= 0) { switch (path[i][j]) { case 3: exctI =
		 * i; exctJ = j; case 0: i--; j--; break; case 1: i--; break; case 2:
		 * j--; break; } } int d = exctI - exctJ; System.out.println(exctI + ","
		 * + exctJ); int nStopWords = 0; if (d > 0) { // count the stop words in
		 * the first d tokens of the query for (i = 0; i < d; i++) { if
		 * (StopWords.contains(query.getQueryTerms()[i])) nStopWords++; } }
		 * System.out.println(nStopWords); System.out.println(min); return min -
		 * d + nStopWords * STOP_WORD_SCORE;
		 */
		return min;
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

	private float minimum(float a, float b, float c) {
		return Math.min(Math.min(a, b), c);
	}

}