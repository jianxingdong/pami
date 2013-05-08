package rankinggraph.scoring;

import java.io.Serializable;
import java.util.HashMap;

/**
 * 
 * links of the exact matching indexes between a pattern and a query
 * 
 */
public class PatternQueryLinks implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private HashMap<Integer, Integer> queryToPattern, patternToQuery;

	public PatternQueryLinks(int patternLength, int queryLength) {
		int initCapacity = Math.min(patternLength, queryLength);
		queryToPattern = new HashMap<Integer, Integer>(initCapacity);
		patternToQuery = new HashMap<Integer, Integer>(initCapacity);
	}

	public void addLink(int queryIndex, int patternIndex) {
		queryToPattern.put(queryIndex, patternIndex);
		patternToQuery.put(patternIndex, queryIndex);
	}

	/**
	 * 
	 * @param patternIndex
	 * @return -1 if no link
	 */
	public int getQueryIndex(int patternIndex) {
		Integer qIndx = patternToQuery.get(patternIndex);
		return qIndx == null ? -1 : qIndx;
	}

	/**
	 * 
	 * @param queryIndex
	 * @return -1 if no link
	 */
	public int getPatternIndex(int queryIndex) {
		Integer pIndx = queryToPattern.get(queryIndex);
		return pIndx == null ? -1 : pIndx;
	}

	@Override
	public String toString() {
		return "queries: " + queryToPattern + ", patterns: " + patternToQuery;
	}
}
