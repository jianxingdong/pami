package rankinggraph.scoring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import parsers.Utils;
import rankinggraph.ParsedQueryReader;
import rankinggraph.QueryInfo;

/**
 * 
 * Given a set of patterns and a list of queries, the class computes performance
 * measures that evaluates the goodness of the patterns.
 * 
 */
public class PatternSetCoverage {

	public float computeCoverage(String queriesFile, String patternsFile)
			throws IOException, ClassNotFoundException {
		List<QueryInfo> queries = new ArrayList<QueryInfo>();
		ParsedQueryReader queriesReader = new ParsedQueryReader(queriesFile);
		QueryInfo queryInfo;
		while ((queryInfo = queriesReader.next()) != null) {
			queries.add(queryInfo);
		}
		queriesReader.close();
		int numQueries = queries.size();
		List<String> patterns = Utils.loadLines(patternsFile);
		// to be changed if sth other than ngram
		PatternQueryMatcher matcher = new NGramPatternMatcher();
		List<QueryInfo> matched = null;
		for (String pattern : patterns) {
			matched = new ArrayList<QueryInfo>();
			for (QueryInfo q : queries) {
				if (matcher.getMatchScore(pattern, q) == 1) {
					matched.add(q);
				}
			}
			queries.removeAll(matched);
			matched.clear();
			if (queries.isEmpty())
				break;
		}
		return ((float) queries.size()) / numQueries;
	}

	/**
	 * 
	 * @param args
	 *            {queriesFile, patternsFile}
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		System.out.println("Coverage: "
				+ new PatternSetCoverage().computeCoverage(args[0], args[1]));
	}
}
