package rankinggraph.scoring;

import java.util.List;

import rankinggraph.QueryInfo;
import rankinggraph.labelpropagation.MatchingRecord;

public interface PatternQueryMatcher {

	public MatchingRecord getMatchScore(List<String> patternTokens,
			QueryInfo query);
}
