package parsers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import rankinggraph.QueryParser;

public class RiccardiWordClassToNE {

	public void convert(String wordClassFile, String queriesFile, String neFile)
			throws IOException {
		BufferedReader wcReader = new BufferedReader(new FileReader(
				wordClassFile));
		BufferedReader queriesReader = new BufferedReader(new FileReader(
				queriesFile));
		BufferedWriter neWriter = new BufferedWriter(new FileWriter(neFile));
		String query, wcLine;
		while ((query = queriesReader.readLine()) != null) {
			wcLine = wcReader.readLine();
			String[] qParts = query.split("\\s");
			String[] wcParts = wcLine.split("\\s");
			int i = 0;
			for (String qPart : qParts) {
				if (qPart.equalsIgnoreCase(wcParts[i])) {
					wcParts[i] = QueryParser.NULL_NE;
				}
				i++;
			}
			neWriter.append(Utils.glueTokens(wcParts) + "\n");
		}
		wcReader.close();
		queriesReader.close();
		neWriter.close();
	}

	/**
	 * 
	 * @param args
	 *            : {wordClassFile, queriesFile, neFile}
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new RiccardiWordClassToNE().convert(args[0], args[1], args[2]);
	}

}
