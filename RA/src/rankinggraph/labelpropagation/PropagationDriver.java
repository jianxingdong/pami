package rankinggraph.labelpropagation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import parsers.Utils;

public class PropagationDriver {
	public static final String NULL_LABEL = "O";

	/**
	 * 
	 * @param patternLabelsFile
	 *            : formatted as {pattern-id}{\\s}{pattern labels, O if a term
	 *            has no label, labels are tokenized in the same way as their
	 *            corresponding pattern}
	 * @param parsedQueriesFile
	 * @param linksFile
	 * @param minMatchScore
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public void propagatePatternLabels(String patternLabelsFile,
			String linksFile, float minMatchScore, String outputFilePath)
			throws IOException, ClassNotFoundException {
		HashMap<Integer, String[]> labeledPatterns = loadInputLabels(patternLabelsFile);
		MatchingRecord.Reader matchesReader = new MatchingRecord.Reader(
				linksFile);
		PQLabelPropagator propagator = new PQLabelPropagator();
		BufferedWriter queryLabelsWriter = new BufferedWriter(new FileWriter(
				outputFilePath));
		MatchingRecord record;
		String[] patternLabels;
		while ((record = matchesReader.next()) != null) {
			if ((patternLabels = labeledPatterns.get(record.getPatternId())) != null
					&& record.getMatchingScore() > minMatchScore) {
				queryLabelsWriter.append(record.getQueryId()
						+ " "
						+ Utils.glueTokens(propagator.getQueryLabels(
								record.getQueryLength(), patternLabels,
								record.getLinks())) + "\n");
			}
		}
		matchesReader.close();
		queryLabelsWriter.close();
	}

	public void propagateQueryLabels(String queryLabelsFile, String linksFile,
			float minMatchScore, String outputFilePath) throws IOException,
			ClassNotFoundException {
		HashMap<Integer, String[]> labeledPatterns = loadInputLabels(queryLabelsFile);
		MatchingRecord.Reader matchesReader = new MatchingRecord.Reader(
				linksFile);
		PQLabelPropagator propagator = new PQLabelPropagator();
		BufferedWriter patternLabelsWriter = new BufferedWriter(new FileWriter(
				outputFilePath));
		MatchingRecord record;
		String[] queryLabels;
		while ((record = matchesReader.next()) != null) {
			if (record.getMatchingScore() > minMatchScore
					&& (queryLabels = labeledPatterns.get(record.getQueryId())) != null) {
				patternLabelsWriter.append(record.getPatternId()
						+ " "
						+ Utils.glueTokens(propagator.getPatternLabels(
								record.getPatternLength(), queryLabels,
								record.getLinks())) + "\n");
			}
		}
		matchesReader.close();
		patternLabelsWriter.close();
	}

	private HashMap<Integer, String[]> loadInputLabels(String inputLabelsFile)
			throws IOException {
		HashMap<Integer, String[]> labels = new HashMap<Integer, String[]>();
		BufferedReader br = new BufferedReader(new FileReader(inputLabelsFile));
		String line;
		String[] parts;
		while ((line = br.readLine()) != null) {
			parts = line.split("\\s");
			String[] labelParts = new String[parts.length - 1];
			for (int i = 1; i < parts.length; i++) {
				if (!parts[i].equalsIgnoreCase(NULL_LABEL)) {
					labelParts[i - 1] = parts[i];
				}
			}
			labels.put(Integer.parseInt(parts[0]), labelParts);
		}
		br.close();
		return labels;
	}

	/**
	 * 
	 * @param args
	 *            : args[0]: qtop/ptoq, args[1] input labels file, args[2] links
	 *            file, args[3] min score, args[4] output labels
	 * @throws IOException
	 * @throws NumberFormatException
	 * @throws ClassNotFoundException
	 */
	public static void _main(String[] args) throws NumberFormatException,
			IOException, ClassNotFoundException {
		if (args[0].equals("ptoq")) {
			new PropagationDriver().propagatePatternLabels(args[1], args[2],
					Float.parseFloat(args[3]), args[4]);
		} else if (args[0].equals("qtop")) {
			new PropagationDriver().propagateQueryLabels(args[1], args[2],
					Float.parseFloat(args[3]), args[4]);
		}
	}

	public static void main(String[] args) throws FileNotFoundException,
			IOException, ClassNotFoundException {
		MatchingRecord.Reader r = new MatchingRecord.Reader(
				"data/Riccardi/parsed/newMatching/matches.txt");
		MatchingRecord m;
		while ((m = r.next()) != null) {
			System.out.println(m.getQueryLength() + "," + m.getPatternLength()
					+ "," + m.getPatternId() + "," + m.getQueryId() + ","
					+ m.getMatchingScore());
		}
	}
}
