package rankinggraph;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import rankinggraph.scoring.NGramPatternMatcher;
import rankinggraph.scoring.PatternQueryMatcher;

public class PatternSupport {
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

	/**
	 * 
	 * @param pattern
	 * @return BitSet: a vector of bits of length equals to number of queries. 1
	 *         means the pattern matches the corresponding query.
	 */
	public BitSet getSupport(String pattern) {
		BitSet matchVctr = new BitSet(nQueries);
		int i = 0;
		for (QueryInfo queryInfo : queries) {
			if (matcher.getMatchScore(pattern, queryInfo) == 1) {
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

	public static void main(String[] args) throws Exception {
		PatternSupport ps = new PatternSupport(new NGramPatternMatcher(),
				"data/Riccardi/parsed/v1_7class_parsedQ.bin");
		BitSet s = ps.getSupport("saturday flight from las-vega to ne-LOC");
		System.out.println(s.isEmpty());
	}
}