package parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * 
 * Parses ATIS0 dataset:
 * http://www.ldc.upenn.edu/Catalog/readme_files/atis/sdtd/trn_prmp.html into a
 * query/line format
 * 
 */
public class ATIS0Parser {

	/**
	 * 
	 * @param srcPath
	 *            : source file path
	 * @param dstPath
	 *            : destination file path
	 * @param removePunctuation
	 *            : if true the (. or ?) at the end of each query is removed
	 * @param lowerCase
	 *            : if true all queries are lowercased
	 * @throws IOException
	 */
	public void parseATIS0(String srcPath, String dstPath,
			boolean removePunctuation, boolean lowerCase) throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(srcPath));
		PrintWriter pw = new PrintWriter(new FileWriter(dstPath));
		String line = null;

		// for the whitespace
		int numCharsToRemove = ((removePunctuation) ? 1 : 0) + 1;
		while ((line = br.readLine()) != null) {
			if (line.length() == 0 || line.startsWith(";")) {
				continue;
			}

			int queryEndIndx = line.lastIndexOf('(') - numCharsToRemove;

			if (lowerCase) {
				pw.println(line.substring(0, queryEndIndx).toLowerCase());
			} else {
				pw.println(line.substring(0, queryEndIndx));
			}
		}
		pw.close();
		br.close();
	}

	/**
	 * 
	 * @param args
	 *            : args[0] srcPath, args[1] dstPath, args[2] true/false remove
	 *            Punctuation, args[3] lowercase
	 * @throws IOException
	 */
	public static void main(String[] args) {
		try {
			new ATIS0Parser().parseATIS0(args[0], args[1],
					Boolean.parseBoolean(args[2]),
					Boolean.parseBoolean(args[3]));
		} catch (IOException ioX) {
			System.out.println("Failed!! - " + ioX.getMessage());
		}
	}
}
