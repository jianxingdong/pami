package parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rankinggraph.QueriesReader;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.process.PTBTokenizer;

public class Utils {

	public static String[] tokenize(String str) {
		StringReader reader = new StringReader(str);
		PTBTokenizer<Word> tokenizer = PTBTokenizer.newPTBTokenizer(reader);
		List<Word> toks = tokenizer.tokenize();
		String[] tokArray = new String[toks.size()];
		int i = 0;
		for (Word t : toks)
			tokArray[i++] = t.word();
		return tokArray;
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
			pw.println(tokenize(line).length);
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
			String[] poses = tokenize(line);
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
			String[] toks = tokenize(line);
			for (String t : toks) {
				if (!t.equals(QueriesReader.NULL_NE)) {
					numNE++;
				}
			}
			pw.println(numNE);
			numNE = 0;
		}
		pw.close();
		br.close();
	}

	public static void main(String[] args) throws IOException {
		String[] tkns = tokenize("q1 ne-q2 q3 pos-q4");
		for(String t:tkns)
			System.out.println(t);
//		numNamedEntities("data/AITS/v1/v1_ATIS0_NER_7class_s2.txt",
//				"data/AITS/v1/stats/7class-ne-per-q.txt");
//		
//		queryLength("data/AITS/v1/v1_ATIS0-Queries.txt",
//				"data/AITS/v1/stats/q-leng.txt");
//		
//		numUniquePOS("data/AITS/v1/v1_ATIS0_POS_s2.txt",
//				"data/AITS/v1/stats/pos-freq.txt");
	}
}
