package rankinggraph.patterngeneration;

import java.io.IOException;
import java.io.PrintWriter;

import parsers.Utils;

import rankinggraph.ParsedQueryReader;

import rankinggraph.QueryInfo;

public class PatternsGenerationDriver implements PatternGenerationNotifiable {

	private PrintWriter patternsWriter;
	private QueryPatternsGenerator patternsGenerator;

	public PatternsGenerationDriver() {
		patternsGenerator = new QueryPatternsGenerator(this);
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
	public void generatePatterns(String parsedQueriesFile, String outputFilePath)
			throws IOException, ClassNotFoundException {

		patternsWriter = new PrintWriter(outputFilePath);
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
	}

	@Override
	public void notifyPattern(String[] pattern) {
		patternsWriter.println(Utils.glueTokens(pattern));
		// System.out.println(Utils.glueTokens(pattern));
	}

	/**
	 * 
	 * @param args
	 *            : {parsedQueriesFile, outputFilePath}
	 */
	public static void main(String[] args) {
		PatternsGenerationDriver driver = new PatternsGenerationDriver();
		try {
			driver.generatePatterns(args[0], args[1]);
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
