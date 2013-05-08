package rankinggraph.scoring;

import rankinggraph.QueryInfo;

public interface PatternQueryMatcher {

	public MatchingRecord getMatchScore(String pattern, QueryInfo query);
}
