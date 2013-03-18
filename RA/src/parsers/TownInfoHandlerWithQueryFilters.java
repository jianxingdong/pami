package parsers;

import org.xml.sax.SAXException;

public class TownInfoHandlerWithQueryFilters extends TownInfoSaxHandler {

	private String query;

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (isTranscription) {
			query = new String(ch, start, length);
			// ignore (one_word)
			query = query.replaceAll("\\(\\S*\\)", "").trim();
			if (query.length() == 0) {
				return;
			}
			// lower case
			queriesWriter.write(query.toLowerCase());
			queriesWriter.println();
		}

	}

}
