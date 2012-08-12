package ca.uwaterloo.cpami.css.dataprep;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * 
 * A Utility class to convert a dataset file from UCI BOW format
 * http://archive.ics
 * .uci.edu/ml/machine-learning-databases/bag-of-words/readme.txt to row per
 * sample format, values in each row are separated by ","
 * 
 */
public class UCIBOWRowPerSamplePrep {

	private static final int WORDS_CHUNK_SIZE = 10;

	/**
	 * 
	 * 8200000 141043 483450157 1 6811 1 1 7106 1 1 14401 1 1 14808 1
	 * 
	 * @throws IOException
	 */
	public void convert(String docWordFile, String outputFilePath)
			throws IOException {

		int numWords = getNumWords(docWordFile);
		int from = 0;
		int to = 0;
		FileWriter fw = new FileWriter(outputFilePath);

		while (from < numWords) {
			to = Math.min(from + WORDS_CHUNK_SIZE - 1, numWords - 1);
			getWordCounts(docWordFile, from, to);
			from = to + 1;
		}
		fw.close();
	}

	private int getNumWords(String docWordFile) throws IOException {
		// second line
		BufferedReader br = new BufferedReader(new FileReader(docWordFile));
		br.readLine();
		return Integer.parseInt(br.readLine());
	}

	/*
	 * private List<String> loadVocab(String vocabFile) throws IOException {
	 * List<String> vocab = new ArrayList<String>(); BufferedReader br = new
	 * BufferedReader(new FileReader(vocabFile)); String line = null; while
	 * ((line = br.readLine()) != null) vocab.add(line); return vocab; }
	 */

	private ArrayList<ArrayList<Integer>> getWordCounts(String docWordFile,
			int offset, int to) throws IOException {

		ArrayList<ArrayList<Integer>> wordVectors = new ArrayList<ArrayList<Integer>>();
		for (int i = offset; i < offset + to; i++) {
			wordVectors.add(new ArrayList<Integer>());
		}
		BufferedReader br = new BufferedReader(new FileReader(docWordFile));
		String line = null;
		// skip first 3 lines
		br.readLine();
		br.readLine();
		br.readLine();
		while ((line = br.readLine()) != null) {
			String[] split = line.split("\\s");
			int wID = Integer.parseInt(split[1]);
			if (offset <= wID && wID >= to) {
				// wordVectors.get(wID-offset).add(e)
			}
		}
		return wordVectors;
	}

	public static void main(String[] args) throws IOException {
		new UCIBOWRowPerSamplePrep().convert(
				"/home/ahmed/Desktop/ICDM13/dataset/docword.pubmed.txt",
				"/home/ahmed/Desktop/ICDM13/dataset/pubmed-rowpersample.txt");
	}
}
