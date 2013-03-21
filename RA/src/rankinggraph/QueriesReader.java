package rankinggraph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class QueriesReader {

	private final static String FILES_SPLIT_REGEX = "\\s+";
	private final static String NULL_NE = "O";

	private BufferedReader queriesReader, posReader, neReader;

	public QueriesReader(String queriesFilePath, String posFilePath,
			String neFilePath) throws FileNotFoundException {

		queriesReader = new BufferedReader(new FileReader(queriesFilePath));
		posReader = new BufferedReader(new FileReader(posFilePath));
		neReader = new BufferedReader(new FileReader(neFilePath));
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

	private QueryInfo parse(String query, String pos, String ne) {
		String[] queryTerms = query.split(FILES_SPLIT_REGEX);
		String[] partOfSpeeches = pos.split(FILES_SPLIT_REGEX);
		String[] namedEntities = ne.split(FILES_SPLIT_REGEX);
		QueryInfo queryInfo = new QueryInfo();
		queryInfo.setQueryTerms(queryTerms);
		queryInfo.setPartOfSpeeches(partOfSpeeches);
		HashMap<Integer, String> dectedNamedEntities = new HashMap<Integer, String>();
		for (int i = 0; i < namedEntities.length; i++) {
			if (!namedEntities[i].equalsIgnoreCase(NULL_NE)) {
				dectedNamedEntities.put(i, namedEntities[i]);
			}
		}
		queryInfo.setNamedEntities(dectedNamedEntities);

		return queryInfo;
	}
}