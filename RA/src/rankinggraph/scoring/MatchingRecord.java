package rankinggraph.scoring;

public class MatchingRecord {

	private float matchingScore;
	private PatternQueryLinks links;

	public MatchingRecord(float matchingScore, PatternQueryLinks links) {
		super();
		this.matchingScore = matchingScore;
		this.links = links;
	}

	public float getMatchingScore() {
		return matchingScore;
	}

	public PatternQueryLinks getLinks() {
		return links;
	}
	

}
