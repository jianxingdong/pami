package rankinggraph.patterngeneration;

import java.util.HashMap;
import java.util.Set;

/**
 * 
 * Generates all the possible patterns for a single query given the query terms,
 * part-of-speeches, and the detected named entities in the query.
 * 
 * A pattern is a list of tokens each of them is a query term, a part-of-speech
 * or a named entity. A valid pattern contains at least one named entity.
 * 
 */
public class QueryPatternsGenerator {

	private PatternGenerationNotifiable generationNotifiable;

	public QueryPatternsGenerator(
			PatternGenerationNotifiable generationNotifiable) {
		this.generationNotifiable = generationNotifiable;
	}

	public void generatePatterns(QueryInfo queryInfo) {
		HashMap<Integer, String> namedEntities = queryInfo.getNamedEntities();
		Set<Integer> keys = namedEntities.keySet();
		String[] seedPattern = new String[queryInfo.getNumTerms()];
		for (int k : keys) {
			seedPattern[k] = namedEntities.get(k);
			enumeratePatterns(queryInfo, seedPattern, k, 0);
		}

	}

	private void enumeratePatterns(QueryInfo queryInfo,
			String[] currentPattern, int initNeIndex, int from) {
		if (from == currentPattern.length) {
			// pattern is ready.
			generationNotifiable.notifyPattern(currentPattern);
		} else if (initNeIndex == from) {
			// already substituted
			enumeratePatterns(queryInfo, currentPattern, initNeIndex, from + 1);
		} else {
			// Substitute something

			// query term
			currentPattern[from] = queryInfo.getQueryTerms()[from];
			enumeratePatterns(queryInfo, currentPattern, initNeIndex, from + 1);

			// or a POS
			currentPattern[from] = "pos:" + queryInfo.getPartOfSpeeches()[from];
			enumeratePatterns(queryInfo, currentPattern, initNeIndex, from + 1);

			// or a NE
			if (from > initNeIndex) {
				String ne = queryInfo.getNamedEntities().get(from);
				if (ne != null) {
					currentPattern[from] = "ne:" + ne;
					enumeratePatterns(queryInfo, currentPattern, initNeIndex,
							from + 1);
				}

			}
		}
	}

	public static void main(String[] args) {
		QueryPatternsGenerator qpg = new QueryPatternsGenerator(
				new PatternsGenerationDriver());
		QueryInfo qi = new QueryInfo();
		qi.setQueryTerms(new String[] { "q1", "q2", "q3" });
		qi.setPartOfSpeeches(new String[] { "p1", "p2", "p3" });
		HashMap<Integer, String> nes = new HashMap<Integer, String>();
		nes.put(1, "n2");
		nes.put(2, "n3");
		qi.setNamedEntities(nes);
		qpg.generatePatterns(qi);
	}
}
