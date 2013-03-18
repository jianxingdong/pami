package parsers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TownInfoSaxHandler extends DefaultHandler {


	protected PrintWriter queriesWriter;
	protected boolean isTranscription = false;

	public void setOutputFile(File outPath) throws FileNotFoundException {
		queriesWriter = new PrintWriter(outPath);
	}

	@Override
	public void startDocument() throws SAXException {
		if (queriesWriter == null) {
			throw new RuntimeException("output file path is not set");
		}
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (qName.equals("transcription")) {
			isTranscription = true;
		}
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (isTranscription) {
			queriesWriter.write(ch, start, length);
			queriesWriter.println();
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (isTranscription) {
			isTranscription = false;
		}
	}

	@Override
	public void endDocument() throws SAXException {
		queriesWriter.close();
	}
}
