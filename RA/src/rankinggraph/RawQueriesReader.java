package rankinggraph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import parsers.Utils;

public class RawQueriesReader {

	public final static String NULL_NE = "O";
	public final static String NULL_POS = ".";

	private BufferedReader queriesReader, posReader, neReader;

	public RawQueriesReader(QueryInfo.QueriesAnnotationFiles queriesFiles)
			throws FileNotFoundException {

		queriesReader = new BufferedReader(new FileReader(
				queriesFiles.getQueriesFilePath()));
		posReader = new BufferedReader(new FileReader(
				queriesFiles.getPosFilePath()));
		neReader = new BufferedReader(new FileReader(
				queriesFiles.getNeFilePath()));
	}

	/**
	 * 
	 * @return null when EOF
	 * @throws IOException
	 */
	public QueryInfo next() throws IOException {
		String query = null;
		if ((query = queriesReader.readLine()) == null) {
			// EOF
			return null;
		}
		return parse(query, posReader.readLine(), neReader.readLine());
	}

	public void close() throws IOException {
		queriesReader.close();
		posReader.close();
		neReader.close();
	}

	// TODO apply design pattern
	private QueryInfo parse(String query, String pos, String ne) {
		List<String> queryTerms = Utils.tokenize(query);
		List<String> partOfSpeeches = Utils.tokenize(pos);
		List<String> namedEntities = Utils.tokenize(ne);
		HashMap<Integer, String> dectedNamedEntities = new HashMap<Integer, String>();

		// delete tokens that are punctuations
		normalizePunct(queryTerms, partOfSpeeches, namedEntities);

		// normalize named entities
		normalizeNETags(namedEntities);

		int numTerms = queryTerms.size();
		// merging consecutive terms with the same named entity
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
		QueryInfo queryInfo = new QueryInfo();
		queryInfo.setQueryTerms(queryTerms.toArray(new String[] {}));
		queryInfo.setPartOfSpeeches(partOfSpeeches.toArray(new String[] {}));
		queryInfo.setNamedEntities(dectedNamedEntities);

		return queryInfo;
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