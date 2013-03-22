package rankinggraph.scoring;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

import rankinggraph.QueriesReader;
import rankinggraph.QueryInfo;

public class ScoringDriver {
	// TODO build P-Q matrix
	// TODO build P-P matrix
	// TODO build Q-Q matrix
	// TODO pattern indexing and query indexing

	// TODO group 3 files in a class
	/**
	 * build a bipartite graph of queries and patterns. The output is qId,PId,1
	 * per line. 0 scores are not written to the file.
	 * 
	 * @param queryFilePath
	 * @param posFilePath
	 * @param neFilePath
	 * @param patternsFile
	 * @param outputFile
	 * @throws IOException
	 */
	public void buildeQueryPatternGraph(String queryFilePath,
			String posFilePath, String neFilePath, String patternsFile,
			String outputFile) throws IOException {

		QueriesReader queriesReader = new QueriesReader(queryFilePath,
				posFilePath, neFilePath);
		QueryInfo queryInfo = null;
		String pattern = null;
		BufferedReader patternsReader = new BufferedReader(new FileReader(
				patternsFile));
		List<String> patterns = new ArrayList<String>();
		while ((pattern = patternsReader.readLine()) != null) {
			patterns.add(pattern);
		}
		patternsReader.close();
		int qId = 0;
		int pId = 0;
		PrintWriter graphWriter = new PrintWriter(new File(outputFile));
		PatternQueryMatcher matcher = new PatternQueryMatcher();
		int numMatches = 0;
		float score = 0;
		while ((queryInfo = queriesReader.next()) != null) {
			pId = 0;
			for (String p : patterns) {
				score = matcher.getExactMatchScore(p, queryInfo);
				if (score == 1) {

					graphWriter.println(qId + "," + pId + "," + score);

					numMatches++;
				}
				pId++;
			}
			System.out.println(qId + "\t" + numMatches);
			qId++;
		}

		graphWriter.close();
	}

	// private final static String GRAPH_OUTPUT_FORMAT = "%d,%d,%f";

	/**
	 * 
	 * @param args
	 *            : {queryFilePath, posFilePath, neFilePath, patternsFile,
	 *            outputFile}
	 */
	public static void main(String[] args) {
		try {
			new ScoringDriver().buildeQueryPatternGraph(args[0], args[1],
					args[2], args[3], args[4]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
