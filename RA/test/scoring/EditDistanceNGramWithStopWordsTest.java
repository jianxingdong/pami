package scoring;

import static org.junit.Assert.*;

import org.junit.Test;

import rankinggraph.QueryInfo;
import rankinggraph.QueryParser;
import rankinggraph.scoring.EditDistanceNGramWithStopWordsMatcher;

public class EditDistanceNGramWithStopWordsTest {

	private EditDistanceNGramWithStopWordsMatcher matcher;
	private QueryInfo query;

	public EditDistanceNGramWithStopWordsTest() {
		matcher = new EditDistanceNGramWithStopWordsMatcher();
		String q = "All flight from Atlanta to San Francisco on Delta first class please.", pos = "DT NNS IN NNP TO NNP NNP IN NNP JJ NN VB .", ne = "O O O LOC-B O LOC-B LOC-I O ORG-B O O O O";
		query = new QueryParser().parse(q, pos, ne);
	}

	@Test
	public void q1() {
		assertEquals(
				1-0.5/6,
				matcher.getMatchScore("from ne-LOC to ne-LOC on ne-ORG", query),
				1e-5);
	}

	@Test
	public void q2() {
		assertEquals(1 - 2f / 3,
				matcher.getMatchScore("dfsk dsfkjs dfskjh", query), 1e-5);
	}

	@Test
	public void q3() {
		assertEquals(1 - 2.5 / 11, matcher.getMatchScore(
				"from xx ne-LOC to ne-LOC the the the the on ne-ORG", query),
				1e-5);
	}

	@Test
	public void q4() {
		assertEquals(1 - 1f / 7, matcher.getMatchScore(
				"the from ne-LOC to ne-LOC on ne-ORG", query), 1e-5);
	}

	@Test
	public void q5() {
		assertEquals(1 - 1f / 7, matcher.getMatchScore(
				"from ne-LOC to ne-LOC ona ne-ORG the", query), 1e-5);
	}

	@Test
	public void q6() {
		assertEquals(1 - 1f / 7, matcher.getMatchScore(
				"xxxx from ne-LOC to ne-LOC on ne-ORG", query), 1e-5);
	}
		
}
