package rankinggraph;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * 
 * reads a parsed query from a binary file
 * 
 */
public class ParsedQueryReader {

	private ObjectInputStream objectInStream;

	public ParsedQueryReader(String filePath) throws FileNotFoundException, IOException {
		objectInStream = new ObjectInputStream(new FileInputStream(filePath));
	}

	private QueryInfo tmpQuery;

	/**
	 * 
	 * @return null when EOF
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public QueryInfo next() throws ClassNotFoundException, IOException {
		try {
			tmpQuery = (QueryInfo) objectInStream.readObject();
		} catch (EOFException e) {
			return null;
		}
		return tmpQuery;
	}

	public void close() throws IOException {
		objectInStream.close();
	}
}
