package rankinggraph.scoring;

public interface PatternMatchNotifiable {

	public void notifyMatch(int queryId, int patternId, float score);
}
