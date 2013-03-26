package rankinggraph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class RawQueriesReader {

	private BufferedReader queriesReader, posReader, neReader;
	private QueryParser queryParser;

	public RawQueriesReader(QueryInfo.QueriesAnnotationFiles queriesFiles)
			throws FileNotFoundException {

		queriesReader = new BufferedReader(new FileReader(
				queriesFiles.getQueriesFilePath()));
		posReader = new BufferedReader(new FileReader(
				queriesFiles.getPosFilePath()));
		neReader = new BufferedReader(new FileReader(
				queriesFiles.getNeFilePath()));
		queryParser = new QueryParser();		
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
		return queryParser.parse(query, posReader.readLine(),
				neReader.readLine());
	}

	public void close() throws IOException {
		queriesReader.close();
		posReader.close();
		neReader.close();
	}

}