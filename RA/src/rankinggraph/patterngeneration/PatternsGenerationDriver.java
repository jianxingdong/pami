package rankinggraph.patterngeneration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

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
		BufferedReader queriesReader = new BufferedReader(new FileReader(
				queriesFilePath));
		BufferedReader posReader = new BufferedReader(new FileReader(
				posFilePath));
		BufferedReader neReader = new BufferedReader(new FileReader(neFilePath));
		patternsWriter = new PrintWriter(outputFilePath);
		String query = null;
		int count = 0;
		while ((query = queriesReader.readLine()) != null) {
			System.out.println(count++);
			QueryInfo queryInfo = createQueryInfo(query, posReader.readLine(),
					neReader.readLine());
			patternsGenerator.generatePatterns(queryInfo);
		}
		queriesReader.close();
		posReader.close();
		neReader.close();
		patternsWriter.close();
	}

	private final static String FILES_SPLIT_REGEX = "\\s+";
	private final static String NULL_NE = "O";

	private QueryInfo createQueryInfo(String query, String pos, String ne) {
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
