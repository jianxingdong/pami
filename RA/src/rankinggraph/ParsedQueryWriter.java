package rankinggraph;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

import parsers.Utils;

import rankinggraph.QueryInfo.QueriesAnnotationFiles;

/**
 * 
 * reads query and its annotations and writes a binary file of parsed and
 * tokenized QueryInfo objects
 * 
 */
public class ParsedQueryWriter {

	public void parseAndWrite(QueryInfo.QueriesAnnotationFiles queriesFiles,
			String outBinaryFilePath) throws IOException {
		RawQueriesReader reader = new RawQueriesReader(queriesFiles);
		ObjectOutputStream objectOutStream = new ObjectOutputStream(
				new FileOutputStream(outBinaryFilePath));
		QueryInfo queryInfo = null;
		int count = 0;
		PrintWriter pw = new PrintWriter("preprocessed-queries.txt");
		while ((queryInfo = reader.next()) != null) {
			System.out.println(count++);
			pw.println(Utils.glueTokens(queryInfo.getQueryTerms()));
			objectOutStream.writeObject(queryInfo);
		}
		pw.close();
		objectOutStream.close();
		reader.close();
	}

	/**
	 * 
	 * @param args
	 *            args[0,1,2] queries files, args[3] output file path
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new ParsedQueryWriter().parseAndWrite(new QueriesAnnotationFiles(
				args[0], args[1], args[2]), args[3]);
	}
}
