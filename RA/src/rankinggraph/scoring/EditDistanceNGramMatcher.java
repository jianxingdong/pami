package rankinggraph.scoring;

import java.util.List;

import parsers.StopWords;
import rankinggraph.QueryInfo;
import rankinggraph.labelpropagation.MatchingRecord;
import rankinggraph.labelpropagation.PatternQueryLinks;
import rankinggraph.patterngeneration.AbstractPatternGenerator;

public class EditDistanceNGramMatcher implements PatternQueryMatcher {

	private boolean withStopWords;

	public EditDistanceNGramMatcher() {
	}

	public EditDistanceNGramMatcher(boolean withStopWords) {
		this.withStopWords = withStopWords;
	}

	@Override
	public MatchingRecord getMatchScore(List<String> patternTokens,
			QueryInfo query) {
		int patternLength = patternTokens.size();
		if (patternLength == 0)
			return new MatchingRecord(0, null);
		MatchingRecord match = withStopWords ? computeLevenshteinDistanceWithStopWords(
				patternTokens, query) : computeLevenshteinDistance(
				patternTokens, query);
		float minLen = Math.min(patternLength, query.getNumTerms());
		float editDistance = match.getMatchingScore();
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
				boolean tknsMatch = matches(patternTokens.get(j - 1), query,
						i - 1);
				ArrayEntry minEntry = minimum(
						distance[i - 1][j] + 1,
						distance[i][j - 1] + 1,
						distance[i - 1][j - 1]
								+ (matches(patternTokens.get(j - 1), query,
										i - 1) ? 0 : 1));
				distance[i][j] = minEntry.value;
				// 0 -> delete, 1-> insert, 2-> substitute, 3-> exact
				path[i - 1][j - 1] = tknsMatch && minEntry.index == 2 ? 3
						: minEntry.index;
			}

		}
		ArrayEntry minLastCol = minOfColumn(distance, 1, queryLen + 1,
				patternLen);
		return new MatchingRecord(minLastCol.value, getLinks(path,
				minLastCol.index, queryLen, patternLen));
	}

	public static final float STOP_WORD_SCORE = 0.5f;

	private MatchingRecord computeLevenshteinDistanceWithStopWords(
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
		ArrayEntry minLastCol = minOfColumn(distance, 1, queryLen + 1,
				patternLen);
		return new MatchingRecord(minLastCol.value, getLinks(path,
				minLastCol.index, queryLen, patternLen));
	}

	private PatternQueryLinks getLinks(int[][] pathMatrix, int minRow,
			int queryLen, int patternLen) {
		// backtracking, searching for the exact matches in the path of the best
		// matching.
		PatternQueryLinks links = new PatternQueryLinks(patternLen, queryLen);
		int i = minRow - 1, j = patternLen - 1;

		while (i >= 0 && j >= 0) {
			switch (pathMatrix[i][j]) {
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
		return links;
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

	private ArrayEntry minOfColumn(float[][] matrix, int fromRow, int numRows,
			int colIndex) {
		float min = Float.MAX_VALUE;
		int indx = -1;
		for (int i = fromRow; i < numRows; i++) {
			if (matrix[i][colIndex] < min) {
				min = matrix[i][colIndex];
				indx = i;
			}
		}
		return new ArrayEntry(min, indx);
	}
}
