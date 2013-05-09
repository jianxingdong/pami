package rankinggraph.scoring;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import parsers.Utils;

import rankinggraph.ParsedQueryReader;
import rankinggraph.QueryInfo;

public class ScoringDriver {
	// TODO build P-Q matrix
	// TODO build P-P matrix
	// TODO build Q-Q matrix
	// TODO pattern indexing and query indexing

	private PatternQueryMatcher matcher;
	{
		matcher = new NGramPatternMatcher();
		// matcher = new ExactMatcher();
	}

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
	 * @throws ClassNotFoundException
	 */
	public void buildeQueryPatternGraph(String parsedQueriesFile,
			String patternsFile, String outputFile) throws IOException,
			ClassNotFoundException {

		ParsedQueryReader queriesReader = new ParsedQueryReader(
				parsedQueriesFile);
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

		int numMatches = 0;
		float score = 0;

		while ((queryInfo = queriesReader.next()) != null) {
			pId = 0;
			for (String p : patterns) {
				score = matcher.getMatchScore(Utils.tokenize(p), queryInfo)
						.getMatchingScore();
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
	 *            : {parsedQueriesFile, patternsFile, outputFile}
	 */
	public static void main(String[] args) {
		try {
			new ScoringDriver().buildeQueryPatternGraph(args[0], args[1],
					args[2]);
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
