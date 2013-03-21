package rankingGraph;

import java.util.HashMap;

public class QueryInfo {

	private int numTerms;
	private String[] queryTerms, partOfSpeeches;

	private HashMap<Integer, String> namedEntities;

	public int getNumTerms() {
		return this.numTerms;
	}

	public String[] getQueryTerms() {
		return queryTerms;
	}

	public void setQueryTerms(String[] queryTerms) {
		this.queryTerms = queryTerms;
		this.numTerms = queryTerms.length;
	}

	public String[] getPartOfSpeeches() {
		return partOfSpeeches;
	}

	public void setPartOfSpeeches(String[] partOfSpeeches) {
		this.partOfSpeeches = partOfSpeeches;
	}

	public HashMap<Integer, String> getNamedEntities() {
		return namedEntities;
	}

	public void setNamedEntities(HashMap<Integer, String> namedEntities) {
		this.namedEntities = namedEntities;
	}
}