package rankinggraph.patterngeneration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import parsers.Utils;

import rankinggraph.ParsedQueryReader;

import rankinggraph.QueryInfo;
import rankinggraph.labelpropagation.MatchingRecord;
import rankinggraph.scoring.EditDistanceNGramMatcher;
import rankinggraph.scoring.PatternMatchNotifiable;

public class PatternsGenerationDriver implements PatternGenerationNotifiable,
		PatternMatchNotifiable, Runnable {

	private PrintWriter patternsWriter, matchesWriter;
	private AbstractPatternGenerator patternsGenerator;
	private MatchingRecord.Writer matchRecordsWriter;

	public PatternsGenerationDriver(String parsedQueriesFile) throws Exception {
		// patternsGenerator = new TagsCombinationGenerator(this);
		// patternsGenerator = new NGramPatternGenerator(this);
		patternsGenerator = new PrunedNGramGenerator(this, parsedQueriesFile,
				this, new EditDistanceNGramMatcher(false));

	}

	/**
	 * 
	 * @param queriesFilePath
	 *            : queries file
	 * @param posFilePath
	 *            : part of speech file
	 * @param neFilePath
	 *            : named entities file
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	QueryProducer queryProducer;

	public void generatePatterns(String parsedQueriesFile,
			String outputFilePath, String matchesFile, String matchRecordsFile)
			throws IOException, ClassNotFoundException {

		patternsWriter = new PrintWriter(outputFilePath);
		matchesWriter = new PrintWriter(matchesFile);
		matchRecordsWriter = new MatchingRecord.Writer(matchRecordsFile);
		queryProducer = new QueryProducer(parsedQueriesFile);
	}

	public void close() throws IOException {
		patternsWriter.close();
		matchesWriter.close();
		matchRecordsWriter.close();
	}

	Integer count = 0;

	public void run() {
		QueryInfo query = null;
		try {
			while ((query = queryProducer.produce()) != null) {
				patternsGenerator.generatePatterns(query);
				synchronized (count) {
					count++;
					System.out.println(count + ", "
							+ Thread.currentThread().getName());
				}
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}

	}

	private class QueryProducer {
		ParsedQueryReader queriesReader;
		boolean isEOF;

		QueryProducer(String queriesFile) throws FileNotFoundException,
				IOException {
			queriesReader = new ParsedQueryReader(queriesFile);
		}

		synchronized QueryInfo produce() throws ClassNotFoundException,
				IOException {
			if (isEOF)
				return null;
			QueryInfo q = queriesReader.next();
			isEOF = q == null;
			if (isEOF)
				queriesReader.close();
			return q;
		}

	}

	Integer patternCount = 0;

	@Override
	public synchronized int notifyPattern(String[] pattern) {
		patternsWriter.println(Utils.glueTokens(pattern));
		return patternCount++;
	}

	@Override
	public void notifyMatch(MatchingRecord m) {
		matchesWriter.println(m.getQueryId() + "," + m.getPatternId() + ","
				+ m.getMatchingScore());
		try {
			matchRecordsWriter.writeRecord(m);
		} catch (IOException e) {
			// TODO refactor
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	/**
	 * 
	 * @param args
	 *            : {parsedQueriesFile, outputFilePath, scoresFile,
	 *            matcingrecordsfile, numThreads}
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		PatternsGenerationDriver driver = new PatternsGenerationDriver(args[0]);
		try {
			// init
			driver.generatePatterns(args[0], args[1], args[2], args[3]);
			List<Thread> threads = new ArrayList<Thread>();
			for (int i = 0; i < Integer.parseInt(args[4]); i++) {
				Thread t = new Thread(driver);
				t.start();
				threads.add(t);
			}
			for (Thread t : threads) {
				t.join();
			}
			driver.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}