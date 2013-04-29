package rankinggraph.patterngeneration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
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
		LinkedHashSet<String> uniquePatterns = new LinkedHashSet<String>();
		String pattern = null;
		int numDuplicates = 0;
		HashSet<Integer> duplicates = new HashSet<Integer>();
		HashMap<Integer, Integer> oldNewIndexMap = new HashMap<Integer, Integer>();
		int newIndex = 0;
		int i = 0;
		while ((pattern = patternsReader.readLine()) != null) {
			if (uniquePatterns.add(pattern)) {
				oldNewIndexMap.put(i, newIndex++);				
			} else {
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

		fixLinks(duplicates, linksFile, fixedLinksFile, oldNewIndexMap);
		return numDuplicates;
	}

	private void fixLinks(HashSet<Integer> dupIndexes, String linksFile,
			String fixedLinksFile, HashMap<Integer, Integer> oldNewIndexMap)
			throws IOException {
		BufferedReader linksReader = new BufferedReader(new FileReader(
				linksFile));
		String link = null;

		PrintWriter linksWriter = new PrintWriter(fixedLinksFile);
		while ((link = linksReader.readLine()) != null) {
			String[] parts = link.split(",");
			int oldIndex = Integer.parseInt(parts[1]);
			if (!dupIndexes.contains(oldIndex)) {
				int newIndex = oldNewIndexMap.get(oldIndex);
				linksWriter.println(parts[0] + "," + newIndex + "," + parts[2]);
			}
		}
		linksReader.close();
		linksWriter.close();
	}

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
