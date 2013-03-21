package rankinggraph.patterngeneration;

import java.io.IOException;
import java.io.PrintWriter;

import rankinggraph.QueriesReader;
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
	 */
	public void generatePatterns(String queriesFilePath, String posFilePath,
			String neFilePath, String outputFilePath) throws IOException {

		patternsWriter = new PrintWriter(outputFilePath);
		QueriesReader queriesReader = new QueriesReader(queriesFilePath,
				posFilePath, neFilePath);
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
		StringBuilder sb = new StringBuilder();
		int lastIndx = pattern.length - 1;
		for (int i = 0; i < lastIndx; i++) {
			sb.append(pattern[i]);
			sb.append(' ');
		}
		sb.append(pattern[lastIndx]);
		patternsWriter.println(sb.toString());
		// System.out.println(sb.toString());
	}

	/**
	 * 
	 * @param args
	 *            : {queriesFilePath, posFilePath, neFilePath, outputFilePath}
	 */
	public static void main(String[] args) {
		PatternsGenerationDriver driver = new PatternsGenerationDriver();
		try {
			driver.generatePatterns(args[0], args[1], args[2], args[3]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
