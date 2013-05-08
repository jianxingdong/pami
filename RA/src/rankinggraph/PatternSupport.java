package rankinggraph;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import rankinggraph.scoring.EditDistanceNGramMatcher;
import rankinggraph.scoring.PatternQueryMatcher;

public class PatternSupport {
	public static final float MIN_MATCH_SCORE = 0.7f;

	private List<QueryInfo> queries;

	private PatternQueryMatcher matcher;
	private int nQueries;

	public PatternSupport(PatternQueryMatcher matcher, String parsedQueriesFile)
			throws Exception {
		this.matcher = matcher;
		queries = new ArrayList<QueryInfo>();
		loadQueries(parsedQueriesFile);
		this.nQueries = queries.size();
	}

	// TODO this is a hack
	public List<Float> scores = new ArrayList<Float>();

	/**
	 * 
	 * @param pattern
	 * @return BitSet: a vector of bits of length equals to number of queries. 1
	 *         means the pattern matches the corresponding query.
	 */
	public BitSet getSupport(String pattern) {
		scores.clear();
		BitSet matchVctr = new BitSet(nQueries);
		int i = 0;
		for (QueryInfo queryInfo : queries) {
			float score = matcher.getMatchScore(pattern, queryInfo)
					.getMatchingScore();
			if (score < 0)
				throw new RuntimeException("invalid score :" + score
						+ " - pattern: " + pattern);
			if (score >= MIN_MATCH_SCORE) {
				matchVctr.set(i);
				scores.add(score);
			}
			++i;
		}
		return matchVctr;
	}

	public void clear() {
		queries.clear();
	}

	private void loadQueries(String parsedQueriesFile)
			throws FileNotFoundException, IOException, ClassNotFoundException {
		ParsedQueryReader queriesReader = new ParsedQueryReader(
				parsedQueriesFile);
		QueryInfo queryInfo;
		while ((queryInfo = queriesReader.next()) != null) {
			queries.add(queryInfo);
		}
		queriesReader.close();
	}

	public static void main(String[] args) throws Exception {
		PatternSupport ps = new PatternSupport(new EditDistanceNGramMatcher(),
				"data/Riccardi/parsed/newMatching/v1-ne-ParsedQ.bin");
		BitSet s = ps
				.getSupport("ne-cost_relative flight from ne-city_name ne-state_code");

		System.out.println(s.isEmpty());
	}
}