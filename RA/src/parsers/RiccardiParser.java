package parsers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import static parsers.Utils.glueTokens;

public class RiccardiParser {

	public void parse(String in, String queriesFile, String wordClassFile,
			String labelsFile) throws IOException {

		String line = null;
		BufferedReader br = new BufferedReader(new FileReader(in));
		PrintWriter queriesWriter = new PrintWriter(new File(queriesFile));
		PrintWriter wordClassWriter = new PrintWriter(new File(wordClassFile));
		PrintWriter labelsWriter = new PrintWriter(new File(labelsFile));
		List<String> words = new ArrayList<String>();
		List<String> wordClasses = new ArrayList<String>();
		List<String> labels = new ArrayList<String>();
		String[] dummy = {};
		int count = 0;
		while ((line = br.readLine()) != null) {
			if (line.length() == 0) {

				queriesWriter.println(glueTokens(words.toArray(dummy)));
				words.clear();

				wordClassWriter.println(glueTokens(wordClasses.toArray(dummy)));
				wordClasses.clear();

				labelsWriter.println(glueTokens(labels.toArray(dummy)));
				labels.clear();

				count++;
			} else {
				String[] parts = line.split("\\s");
				words.add(parts[0]);
				wordClasses.add(parts[1]);
				labels.add(parts[2]);
			}
		}
		br.close();
		queriesWriter.close();
		wordClassWriter.close();
		labelsWriter.close();
		System.out.println("Number of Queries: " + count);
	}

	public static void main(String[] args) throws IOException {
		new RiccardiParser().parse(args[0], args[1], args[2], args[3]);
	}
}
