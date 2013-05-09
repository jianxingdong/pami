package rankinggraph.scoring;

import rankinggraph.labelpropagation.MatchingRecord;

public interface PatternMatchNotifiable {

	public void notifyMatch(MatchingRecord m);
}
