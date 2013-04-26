package rankinggraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.stanford.nlp.process.Morphology;

import parsers.Utils;

public class QueryParser {

	public final static String NULL_NE = "O";
	public final static String NULL_POS = ".";

	private Morphology morphology;

	{
		morphology = new Morphology();
	}

	public QueryInfo parse(String query, String pos, String ne) {
		List<String> queryTerms = Utils.tokenize(query);
		int numTerms = queryTerms.size();
		List<String> partOfSpeeches = new ArrayList<String>();
		for (int i = 0; i < numTerms; i++)
			partOfSpeeches.add(NULL_POS);
		// TODO Utils.tokenize(pos); instead of the above 3 lines
		List<String> namedEntities = Utils.tokenize(ne);

		// delete tokens that are punctuation
		normalizePunct(queryTerms, partOfSpeeches, namedEntities);

		// lowercase terms

		// stem query terms
		stemQueryTerms(queryTerms);

		// normalize named entities
		normalizeNETags(namedEntities);

		mergeConsecutiveNETerms(queryTerms, namedEntities);

		HashMap<Integer, String> dectedNamedEntities = new HashMap<Integer, String>();
		// ignore null terms and named entities
		ignoreNullTermsAndNE(queryTerms, partOfSpeeches, namedEntities,
				dectedNamedEntities);

		QueryInfo queryInfo = new QueryInfo();
		queryInfo.setQueryTerms(queryTerms.toArray(new String[] {}));
		queryInfo.setPartOfSpeeches(partOfSpeeches.toArray(new String[] {}));
		queryInfo.setNamedEntities(dectedNamedEntities);

		return queryInfo;
	}

	private void stemQueryTerms(List<String> queryTerms) {
		int i = 0, numTerms = queryTerms.size();
		for (; i < numTerms; i++) {
			queryTerms.set(i, morphology.stem(queryTerms.get(i)));
		}
	}

	private void ignoreNullTermsAndNE(List<String> queryTerms,
			List<String> partOfSpeeches, List<String> namedEntities,
			HashMap<Integer, String> dectedNamedEntities) {
		int numTerms = queryTerms.size();
		int termsCount = 0;
		int termsIndex = 0;
		for (int i = 0; i < numTerms; i++) {
			if (queryTerms.get(termsIndex) == null) {
				queryTerms.remove(termsIndex);
				partOfSpeeches.remove(termsIndex);
			} else {
				termsIndex++;
				if (!namedEntities.get(i).equals(NULL_NE)) {
					dectedNamedEntities.put(termsCount, namedEntities.get(i));
				}
				termsCount++;
			}
		}

	}

	/**
	 * merging consecutive terms with the same named entity
	 * 
	 * @param queryTerms
	 * @param namedEntities
	 */
	private void mergeConsecutiveNETerms(List<String> queryTerms,
			List<String> namedEntities) {

		int numTerms = queryTerms.size();

		String prevNE = null, tmpNE = null;
		int startNESequence = 0;
		for (int i = 0; i < numTerms; i++) {
			while (i < numTerms
					&& !(tmpNE = namedEntities.get(i)).equals(NULL_NE)
					&& tmpNE.equals(prevNE)) {
				mergeTerms(queryTerms, startNESequence, i);
				i++;
			}
			startNESequence = i;
			prevNE = tmpNE;
		}

	}

	private void normalizePunct(List<String> queryTerms,
			List<String> partOfSpeeches, List<String> namedEntities) {
		int numTerms = queryTerms.size();
		int termIndex = 0;
		for (int i = 0; i < numTerms; i++) {
			if (queryTerms.get(termIndex).matches("\\p{Punct}")) {
				queryTerms.remove(termIndex);
				partOfSpeeches.remove(termIndex);
				namedEntities.remove(termIndex);
			} else {
				termIndex++;
			}
		}

	}

	private final static String MERGE_SEP = "-";

	/**
	 * appends terms(index2) to terms(index1) and set terms(index2), to null.
	 * 
	 * @param terms
	 * @param partOfSpeeches
	 * @param index1
	 * @param index2
	 */
	private void mergeTerms(List<String> terms, int index1, int index2) {
		terms.set(index1, terms.get(index1) + MERGE_SEP + terms.get(index2));
		terms.set(index2, null);
	}

	/**
	 * NE-B and NE-I are converted to NE
	 * 
	 * @param neTags
	 */

	private static void normalizeNETags(List<String> neTags) {
		int size = neTags.size();
		String tmp = null;
		for (int i = 0; i < size; i++) {
			int in = (tmp = neTags.get(i)).indexOf("-");
			if (in != -1)
				neTags.set(i, tmp.substring(0, in));
		}
	}

}