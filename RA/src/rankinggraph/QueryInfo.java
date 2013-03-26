package rankinggraph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class QueryInfo implements Serializable {

	public static class QueriesAnnotationFiles {
		private String queriesFilePath, posFilePath, neFilePath;

		public QueriesAnnotationFiles(String queriesFilePath,
				String posFilePath, String neFilePath) {
			this.queriesFilePath = queriesFilePath;
			this.posFilePath = posFilePath;
			this.neFilePath = neFilePath;
		}

		public String getQueriesFilePath() {
			return queriesFilePath;
		}

		public String getPosFilePath() {
			return posFilePath;
		}

		public String getNeFilePath() {
			return neFilePath;
		}

	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
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

	/**
	 * 
	 * @param namedEntity
	 * @param startMatchFrom
	 * @return -1 if not found
	 */
	public int getNamedEntityIndex(String namedEntity, int startMatchFrom) {
		for (Map.Entry<Integer, String> neEntry : namedEntities.entrySet()) {
			if (neEntry.getKey() >= startMatchFrom
					&& namedEntity.equals(neEntry.getValue())) {
				return neEntry.getKey();
			}
		}
		return -1;
	}
}