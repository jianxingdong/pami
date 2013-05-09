package rankinggraph.labelpropagation;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@SuppressWarnings("serial")
public class MatchingRecord implements Serializable {

	private float matchingScore;
	private PatternQueryLinks links;
	private int patternLength, queryLength, patternId, queryId;

	public MatchingRecord(float matchingScore, PatternQueryLinks links) {
		this.matchingScore = matchingScore;
		this.links = links;
	}

	public float getMatchingScore() {
		return matchingScore;
	}

	public void setMatchingScore(float score) {
		this.matchingScore = score;
	}

	public PatternQueryLinks getLinks() {
		return links;
	}

	public int getPatternLength() {
		return patternLength;
	}

	public void setPatternLength(int patternLength) {
		this.patternLength = patternLength;
	}

	public int getQueryLength() {
		return queryLength;
	}

	public void setQueryLength(int queryLength) {
		this.queryLength = queryLength;
	}

	public int getPatternId() {
		return patternId;
	}

	public void setPatternId(int patternId) {
		this.patternId = patternId;
	}

	public int getQueryId() {
		return queryId;
	}

	public void setQueryId(int queryId) {
		this.queryId = queryId;
	}

	public void setLinks(PatternQueryLinks links) {
		this.links = links;
	}

	public static class Reader {
		private ObjectInputStream ois;

		public Reader(String filePath) throws FileNotFoundException,
				IOException {
			ois = new ObjectInputStream(new FileInputStream(filePath));
		}

		public MatchingRecord next() throws ClassNotFoundException, IOException {
			try {
				MatchingRecord record = (MatchingRecord) ois.readObject();
				return record;
			} catch (EOFException x) {
				return null;
			}

		}

		public void close() throws IOException {
			ois.close();
		}
	}

	public static class Writer {
		private ObjectOutputStream oos;

		public Writer(String filePath) throws FileNotFoundException,
				IOException {
			oos = new ObjectOutputStream(new FileOutputStream(filePath));
		}

		public synchronized void writeRecord(MatchingRecord record) throws IOException {
			oos.writeObject(record);
		}

		public void close() throws IOException {
			oos.close();
		}
	}

}
