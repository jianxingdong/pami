package parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class StopWords {

	private static final HashSet<String> STOP_WORDS;

	static {
		STOP_WORDS = new HashSet<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(
					"resources/common-english-words.txt"));
			String l = br.readLine();
			br.close();
			for (String w : l.split(",")) {
				STOP_WORDS.add(w.toLowerCase());
			}
		} catch (IOException x) {
			throw new RuntimeException("error loading stop words");
		}
	}

	public static boolean contains(String word) {
		return STOP_WORDS.contains(word.toLowerCase());
	}

}
