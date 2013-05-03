package rankinggraph.scoring;

import java.util.List;

import parsers.Utils;
import rankinggraph.QueryInfo;
import rankinggraph.patterngeneration.AbstractPatternGenerator;

public class EditDistanceNGramMatcher implements PatternQueryMatcher {

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
											// revisit
	}

	public float computeLevenshteinDistance(List<String> patternTokens,
			QueryInfo query) {
		int queryLen = query.getNumTerms();
		int patternLen = patternTokens.size();
		float[][] distance = new float[queryLen + 1][patternLen + 1];
		int[][] path = new int[queryLen][patternLen];

		/*
		 * for (int i = 0; i <= queryLen; i++) distance[i][0] = 0; //TODO
		 */
		for (int j = 1; j <= patternLen; j++)
			distance[0][j] = j;

		for (int i = 1; i <= queryLen; i++) {

			for (int j = 1; j <= patternLen; j++) {
				distance[i][j] = minimum(
						distance[i - 1][j] + 1,
						distance[i][j - 1] + 1,
						distance[i - 1][j - 1]
								+ (matches(patternTokens.get(j - 1), query,
										i - 1) ? 0 : 1));
				if (distance[i][j] == distance[i - 1][j - 1]) {
					path[i - 1][j - 1] = 3; // match
				} else if (distance[i][j] == distance[i][j - 1] + 1) {
					path[i - 1][j - 1] = 2; // delete
				} else if (distance[i][j] == distance[i - 1][j - 1] + 1) {
					path[i - 1][j - 1] = 0; // replace
				} else {
					path[i - 1][j - 1] = 1; // insert
				}

			}

		}

		float min = Integer.MAX_VALUE;
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
		 * j--; break; } } int d = exctI - exctJ;
		 */
		return min;// TODO- d;
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
