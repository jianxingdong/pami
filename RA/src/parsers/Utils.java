package parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rankinggraph.QueryParser;
import edu.stanford.nlp.process.PTBTokenizer;

public class Utils {

	public static int find(String[] array, String value, int from) {
		for (; from < array.length; from++)
			if (array[from].equalsIgnoreCase(value))
				return from;
		return -1;
	}

	public static List<String> tokenize(String str) {
		// TODO for WC

		/*
		 * StringReader reader = new StringReader(str); PTBTokenizer<Word>
		 * tokenizer = PTBTokenizer.newPTBTokenizer(reader); List<Word> toks =
		 * tokenizer.tokenize(); List<String> tokStrList = new
		 * ArrayList<String>(toks.size()); for (Word t : toks)
		 * tokStrList.add(t.word()); return tokStrList;
		 */
		return tokenizeOnSpace(str);
	}

	public static List<String> tokenizeOnSpace(String str) {
		String[] parts = str.split("\\s");
		List<String> list = new ArrayList<String>(parts.length);
		for (String p : parts)
			list.add(p);
		return list;
	}

	public static String glueTokens(String[] toks) {
		return PTBTokenizer.ptb2Text(Arrays.asList(toks));
	}

	public static void queryLength(String queryFilePath, String outputFile)
			throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(queryFilePath));
		PrintWriter pw = new PrintWriter(outputFile);
		String line = null;
		while ((line = br.readLine()) != null) {
			pw.println(tokenize(line).size());
		}
		pw.close();
		br.close();

	}

	public static void numUniquePOS(String posFilePath, String outputFile)
			throws IOException {
		HashMap<String, Integer> posCoutMap = new HashMap<String, Integer>();
		BufferedReader br = new BufferedReader(new FileReader(posFilePath));
		String line = null;
		while ((line = br.readLine()) != null) {
			List<String> poses = tokenize(line);
			for (String p : poses) {
				Integer count = posCoutMap.get(p);
				if (count == null)
					count = 0;
				posCoutMap.put(p, count + 1);
			}
		}
		br.close();
		PrintWriter pw = new PrintWriter(outputFile);
		for (Map.Entry<String, Integer> e : posCoutMap.entrySet()) {
			pw.println(e.getKey() + " " + e.getValue());
		}
		pw.close();

	}

	public static void numNamedEntities(String neFilePath, String outputFile)
			throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(neFilePath));
		PrintWriter pw = new PrintWriter(outputFile);
		String line = null;
		int numNE = 0;
		while ((line = br.readLine()) != null) {
			List<String> toks = tokenize(line);
			for (String t : toks) {
				if (!t.equals(QueryParser.NULL_NE)) {
					numNE++;
				}
			}
			pw.println(numNE);
			numNE = 0;
		}
		pw.close();
		br.close();
	}

	public static List<String> loadLines(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		List<String> lines = new ArrayList<String>();
		while ((line = reader.readLine()) != null) {
			lines.add(line);
		}
		reader.close();
		return lines;
	}

	public static void main(String[] args) throws IOException {
		// numNamedEntities("data/AITS/v1/v1_ATIS0_NER_7class_s2.txt",
		// "data/AITS/v1/stats/7class-ne-per-q.txt");
		//
		// queryLength("data/AITS/v1/v1_ATIS0-Queries.txt",
		// "data/AITS/v1/stats/q-leng.txt");
		//
		// numUniquePOS("data/AITS/v1/v1_ATIS0_POS_s2.txt",
		// "data/AITS/v1/stats/pos-freq.txt");
	}

	public static int getMin(int[] nums) {
		int min = Integer.MAX_VALUE;
		for (int n : nums)
			if (n < min)
				min = n;
		return min;
	}
}
