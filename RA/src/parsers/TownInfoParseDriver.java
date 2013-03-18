package parsers;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Formatter;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;


public class TownInfoParseDriver {

	public int parseDirectory(String srcDirPath, String outputDirPath,
			boolean applyFilters) throws Exception {

		File srcDir = new File(srcDirPath);
		if (!srcDir.exists()) {
			throw new FileNotFoundException(srcDirPath);
		}
		TownInfoXMLParser townInfoParser;
		int numParsedFiles = 0;
		int totalNumFiles = 0;
		String currentFileName = null;

		try {
			townInfoParser = new TownInfoXMLParser(applyFilters);
			File[] transcriptionFiles = srcDir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.getName().endsWith(".xml");
				}
			});
			totalNumFiles = transcriptionFiles.length;
			for (File f : transcriptionFiles) {
				currentFileName = f.getName();
				String outFileName = currentFileName.substring(0,
						currentFileName.lastIndexOf(".xml")) + ".txt";
				townInfoParser.parseFile(f,
						new File(outputDirPath, outFileName));
				numParsedFiles++;
			}

		} catch (ParserConfigurationException | SAXException | IOException e) {
			e.printStackTrace();
			throw new Exception(new Formatter().format(EXCEPTION_MSG,
					numParsedFiles, totalNumFiles, currentFileName,
					e.getMessage()).toString());
		}
		return numParsedFiles;
	}

	private final static String EXCEPTION_MSG = "Parse Failed - Parsed: %d/%d files - current File: %s - Message: %s";

	/**
	 * 
	 * @param args
	 *            : args[0] source dir, args[1] output dir, args[2]: true/false
	 *            if apply filters
	 */
	public static void main(String[] args) {
		TownInfoParseDriver parseDriver = new TownInfoParseDriver();
		try {
			int numParsed = parseDriver.parseDirectory(args[0], args[1],
					Boolean.parseBoolean(args[2]));
			System.out.println("Parsed Files: " + numParsed);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}