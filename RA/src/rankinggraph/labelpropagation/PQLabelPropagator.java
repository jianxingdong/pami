package rankinggraph.labelpropagation;

import rankinggraph.scoring.PatternQueryLinks;

/**
 * 
 * propagates labels from a query to a pattern or vice-versa
 * 
 */
public class PQLabelPropagator {

	public String[] getPatternLabels(int patternLength, String[] queryLabels,
			PatternQueryLinks links) {
		String[] patternLabels = new String[patternLength];
		for (int i = 0; i < queryLabels.length; i++) {
			if (queryLabels[i] != null) {
				int pIndex = links.getPatternIndex(i);
				if (pIndex != -1) {
					patternLabels[pIndex] = queryLabels[i];
				}
			}
		}
		return patternLabels;
	}

	public String[] getQueryLabels(int queryLength, String[] patternLabels,
			PatternQueryLinks links) {
		String[] queryLabels = new String[queryLength];
		for (int i = 0; i < patternLabels.length; i++) {
			if (patternLabels[i] != null) {
				int qIndex = links.getQueryIndex(i);
				if (qIndex != -1) {
					queryLabels[qIndex] = patternLabels[i];
				}
			}
		}
		return queryLabels;
	}
}
