package parsers;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class TownInfoXMLParser {

	private XMLReader xmlReader;
	private TownInfoSaxHandler townInfoHandler;

	public TownInfoXMLParser() throws ParserConfigurationException,
			SAXException {
		this(false);
	}

	public TownInfoXMLParser(boolean applyFilters)
			throws ParserConfigurationException, SAXException {

		SAXParserFactory spf = SAXParserFactory.newInstance();

		SAXParser saxParser = spf.newSAXParser();
		xmlReader = saxParser.getXMLReader();
		spf.setNamespaceAware(true);
		townInfoHandler = (applyFilters) ? new TownInfoHandlerWithQueryFilters()
				: new TownInfoSaxHandler();
		xmlReader.setContentHandler(townInfoHandler);
	}

	public void parseFile(File srcFile, File dstFile)
			throws MalformedURLException, IOException, SAXException {
		townInfoHandler.setOutputFile(dstFile);
		xmlReader.parse(srcFile.toURI().toURL().getPath());
	}

}