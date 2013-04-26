package rankinggraph.patterngeneration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.LinkedHashSet;

public class DuplicatePatternsRemover {

	/**
	 * 
	 * @param inputFilePath
	 * @param outputFilePath
	 * @return: number of detected duplicates
	 * @throws IOException
	 */
	public int removeDuplicates(String inputFilePath, String outputFilePath,
			String linksFile, String fixedLinksFile) throws IOException {
		BufferedReader patternsReader = new BufferedReader(new FileReader(
				inputFilePath));
		LinkedHashSet<String> uniquePatterns = new LinkedHashSet<>();
		String pattern = null;
		int numDuplicates = 0;
		HashSet<Integer> duplicates = new HashSet<Integer>();
		int i = 0;
		while ((pattern = patternsReader.readLine()) != null) {
			if (!uniquePatterns.add(pattern)) {
				numDuplicates++;
				duplicates.add(i);
			}
			++i;
		}
		patternsReader.close();
		PrintWriter patternsWriter = new PrintWriter(outputFilePath);
		for (String uPattern : uniquePatterns) {
			patternsWriter.println(uPattern);
		}
		patternsWriter.close();

		fixLinks(duplicates, linksFile, fixedLinksFile);
		return numDuplicates;
	}

	private void fixLinks(HashSet<Integer> dupIndexes, String linksFile,
			String fixedLinksFile) throws IOException {
		BufferedReader linksReader = new BufferedReader(new FileReader(
				linksFile));
		String link = null;
		PrintWriter linksWriter = new PrintWriter(fixedLinksFile);
		while ((link = linksReader.readLine()) != null) {
			if (!dupIndexes.contains(Integer.parseInt(link.split(",")[1]))) {
				linksWriter.println(link);
			}
		}
		linksReader.close();
		linksWriter.close();

	}
//data/Riccardi/parsed/v1_queries_Raymond.txt dummy data/Riccardi/parsed/v1_queries_Raymond_NER3class_s2.txt data/Riccardi/parsed/v1_3class_parsedQ.bin
	/**
	 * 
	 * @param args
	 *            : args[0] input patterns file, args[1] no duplicates output,
	 *            args[2] links file, args[3] new links file after removing
	 *            duplicate patterns
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		int numDuplicates = new DuplicatePatternsRemover().removeDuplicates(
				args[0], args[1], args[2], args[3]);
		System.out.println(numDuplicates);
	}
}
