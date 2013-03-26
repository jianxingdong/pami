package rankinggraph.patterngeneration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;

public class DuplicatePatternsRemover {

	/**
	 * 
	 * @param inputFilePath
	 * @param outputFilePath
	 * @return: number of detected duplicates
	 * @throws IOException
	 */
	public int removeDuplicates(String inputFilePath, String outputFilePath)
			throws IOException {
		BufferedReader patternsReader = new BufferedReader(new FileReader(
				inputFilePath));
		HashSet<String> uniquePatterns = new HashSet<>();
		String pattern = null;
		int numDuplicates = 0;
		while ((pattern = patternsReader.readLine()) != null) {
			if (!uniquePatterns.add(pattern)) {
				numDuplicates++;
			}
		}
		patternsReader.close();
		PrintWriter patternsWriter = new PrintWriter(outputFilePath);
		for (String uPattern : uniquePatterns) {
			patternsWriter.println(uPattern);
		}
		patternsWriter.close();

		return numDuplicates;
	}

	/**
	 * 
	 * @param args
	 *            : args[0] input patterns file, args[1] no duplicates output
	 *            file
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		int numDuplicates = new DuplicatePatternsRemover().removeDuplicates(args[0], args[1]);
		System.out.println(numDuplicates);
	}
}
