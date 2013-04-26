package rankinggraph.patterngeneration;

import java.io.IOException;
import java.io.PrintWriter;

import parsers.Utils;

import rankinggraph.ParsedQueryReader;

import rankinggraph.QueryInfo;
import rankinggraph.scoring.PatternMatchNotifiable;

public class PatternsGenerationDriver implements PatternGenerationNotifiable,
		PatternMatchNotifiable {

	private PrintWriter patternsWriter, matchesWriter;
	private AbstractPatternGenerator patternsGenerator;

	public PatternsGenerationDriver(String parsedQueriesFile) throws Exception {
		// patternsGenerator = new TagsCombinationGenerator(this);
		// patternsGenerator = new NGramPatternGenerator(this);
		patternsGenerator = new PrunedNGramGenerator(this, parsedQueriesFile,
				this);
	}

	/**
	 * 
	 * @param queriesFilePath
	 *            : queries file
	 * @param posFilePath
	 *            : part of speech file
	 * @param neFilePath
	 *            : named entities file
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public void generatePatterns(String parsedQueriesFile,
			String outputFilePath, String matchesFile) throws IOException,
			ClassNotFoundException {

		patternsWriter = new PrintWriter(outputFilePath);
		matchesWriter = new PrintWriter(matchesFile);
		ParsedQueryReader queriesReader = new ParsedQueryReader(
				parsedQueriesFile);
		QueryInfo query = null;
		int count = 0;
		while ((query = queriesReader.next()) != null) {
			System.out.println(count++);
			patternsGenerator.generatePatterns(query);
		}
		queriesReader.close();
		patternsWriter.close();
		matchesWriter.close();
	}

	@Override
	public void notifyPattern(String[] pattern) {
		patternsWriter.println(Utils.glueTokens(pattern));
		// System.out.println(Utils.glueTokens(pattern));
	}

	/**
	 * 
	 * @param args
	 *            : {parsedQueriesFile, outputFilePath, scoresFile}
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		PatternsGenerationDriver driver = new PatternsGenerationDriver(args[0]);
		try {
			driver.generatePatterns(args[0], args[1], args[2]);
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void notifyMatch(int queryId, int patternId, float score) {
		matchesWriter.println(queryId + "," + patternId + "," + score);

	}
}
