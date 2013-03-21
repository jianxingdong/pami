package rankinggraph.scoring;

import rankinggraph.QueryInfo;

public class PatternQueryMatcher {

	public float getExactMatchScore(String pattern, QueryInfo query) {
		//TODO
		return 0;
	}

	public float getEditDistanceScore(String pattern, QueryInfo query) {
		// TODO edit distance, lower-case, normalize by query length
		return 0;
	}
}
