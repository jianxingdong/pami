package rankinggraph.scoring;

import rankinggraph.QueryInfo;

public interface PatternQueryMatcher {

	public float getMatchScore(String pattern, QueryInfo query);
}
