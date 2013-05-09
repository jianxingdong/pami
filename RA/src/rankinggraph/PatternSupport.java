package rankinggraph;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import parsers.Utils;

import rankinggraph.labelpropagation.MatchingRecord;
import rankinggraph.scoring.PatternMatchNotifiable;
import rankinggraph.scoring.PatternQueryMatcher;

public class PatternSupport {
	public static final float MIN_MATCH_SCORE = 0.7f;

	private List<QueryInfo> queries;

	private PatternQueryMatcher matcher;
	private int nQueries;

	private PatternMatchNotifiable matchNotifiable;

	public PatternSupport(PatternQueryMatcher matcher,
			String parsedQueriesFile, PatternMatchNotifiable matchNotifiable)
			throws Exception {
		this.matcher = matcher;
		queries = new ArrayList<QueryInfo>();
		loadQueries(parsedQueriesFile);
		this.nQueries = queries.size();
		this.matchNotifiable = matchNotifiable;
	}

	public void match(int patternId, List<String> patternTokens) {
		int i = 0;
		for (QueryInfo queryInfo : queries) {
			MatchingRecord m = matcher.getMatchScore(patternTokens, queryInfo);
			float score = m.getMatchingScore();
			if (score < 0)
				throw new RuntimeException("invalid score :"
						+ score
						+ " - pattern: "
						+ Utils.glueTokens(patternTokens
								.toArray(new String[] {})));
			if (score >= MIN_MATCH_SCORE) {
				m.setQueryId(i);
				m.setPatternId(patternId);
				m.setPatternLength(patternTokens.size());
				m.setQueryId(queryInfo.getNumTerms());
				matchNotifiable.notifyMatch(m);
			}
			++i;
		}
	}

	/**
	 * 
	 * @param pattern
	 * @return BitSet: a vector of bits of length equals to number of queries. 1
	 *         means the pattern matches the corresponding query.
	 */
	@SuppressWarnings("unused")
	private BitSet getSupport(int patternId, List<String> patternTokens) {
		// TODO to be revisited
		BitSet matchVctr = new BitSet(nQueries);
		int i = 0;
		for (QueryInfo queryInfo : queries) {
			MatchingRecord m = matcher.getMatchScore(patternTokens, queryInfo);
			float score = m.getMatchingScore();
			if (score < 0)
				throw new RuntimeException("invalid score :"
						+ score
						+ " - pattern: "
						+ Utils.glueTokens(patternTokens
								.toArray(new String[] {})));
			if (score >= MIN_MATCH_SCORE) {
				matchVctr.set(i);
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

}