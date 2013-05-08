package scoring;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rankinggraph.QueryInfo;
import rankinggraph.QueryParser;
import rankinggraph.labelpropagation.PQLabelPropagator;
import rankinggraph.scoring.EditDistanceNGramWithStopWordsMatcher;
import rankinggraph.scoring.MatchingRecord;
import rankinggraph.scoring.PatternQueryLinks;

public class LabelPropagationTest {

	private EditDistanceNGramWithStopWordsMatcher matcher;
	private QueryInfo query;

	public LabelPropagationTest() {
		matcher = new EditDistanceNGramWithStopWordsMatcher();
		String q = "All flight from Atlanta to San Francisco on Delta first class please.", pos = "DT NNS IN NNP TO NNP NNP IN NNP JJ NN VB .", ne = "O O O LOC-B O LOC-B LOC-I O ORG-B O O O O";
		query = new QueryParser().parse(q, pos, ne);
	}

	@Test
	public void queryLabeling1() {
		MatchingRecord match = matcher.getMatchScore(
				"from ne-LOC to ne-LOC on ne-ORG", query);
		PatternQueryLinks links = match.getLinks();
		String[] patternLabels = { null, "fromLoc", null, "toLoc", null,
				"Company" };
		String[] queryLabels = new PQLabelPropagator().getQueryLabels(
				query.getNumTerms(), patternLabels, links);
		for (int i : new Integer[] { 0, 1, 2, 4, 6, 8, 9, 10 }) {
			assertEquals(queryLabels[i], null);
		}
		assertEquals(queryLabels[3], "fromLoc");
		assertEquals(queryLabels[5], "toLoc");
		assertEquals(queryLabels[7], "Company");
	}

	@Test
	public void queryLabeling2() {
		MatchingRecord match = matcher.getMatchScore("dfsk dsfkjs dfskjh",
				query);
		PatternQueryLinks links = match.getLinks();
		String[] patternLabels = { null, null, null, };
		String[] queryLabels = new PQLabelPropagator().getQueryLabels(
				query.getNumTerms(), patternLabels, links);
		for (int i : new Integer[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }) {
			assertEquals(queryLabels[i], null);
		}
	}

	@Test
	public void queryLabeling3() {
		MatchingRecord match = matcher.getMatchScore(
				"from xxx ne-LOC to ne-LOC the the the the on ne-ORG", query);
		PatternQueryLinks links = match.getLinks();
		String[] patternLabels = { null, null, "fromLoc", null, "toLoc", null,
				null, null, null, null, "company" };
		String[] queryLabels = new PQLabelPropagator().getQueryLabels(
				query.getNumTerms(), patternLabels, links);
		for (int i : new Integer[] { 0, 1, 2, 4, 6, 8, 9, 10 }) {
			assertEquals(queryLabels[i], null);
		}
		assertEquals(queryLabels[3], "fromLoc");
		assertEquals(queryLabels[5], "toLoc");
		assertEquals(queryLabels[7], "company");
	}

	@Test
	public void patternLabeling() {
		MatchingRecord match = matcher.getMatchScore(
				"from xxx ne-LOC to ne-LOC the the the the on ne-ORG", query);
		PatternQueryLinks links = match.getLinks();
		String[] queryLabels = { null, null, null, "fromLoc", null, "toLoc",
				null, "company", null, null, null };

		String[] patternLabels = new PQLabelPropagator().getPatternLabels(
				query.getNumTerms(), queryLabels, links);
		for (int i : new Integer[] { 0, 1, 3, 5, 6, 7, 8, 9 }) {
			assertEquals(patternLabels[i], null);
		}
		assertEquals(patternLabels[2], "fromLoc");
		assertEquals(patternLabels[4], "toLoc");
		assertEquals(patternLabels[10], "company");
	}
}
