package scoring;

import static org.junit.Assert.*;

import org.junit.Test;

import rankinggraph.QueryInfo;
import rankinggraph.QueryParser;
import rankinggraph.scoring.EditDistanceNGramMatcher;

public class EditDistanceNGramTest {

	private EditDistanceNGramMatcher matcher;
	private QueryInfo query;

	public EditDistanceNGramTest() {
		matcher = new EditDistanceNGramMatcher();
		String q = "All flight from Atlanta to San Francisco on Delta first class please.", pos = "DT NNS IN NNP TO NNP NNP IN NNP JJ NN VB .", ne = "O O O LOC-B O LOC-B LOC-I O ORG-B O O O O";
		// String q =
		// "i'd like to find the cheapest flight from washington dc to atlanta",
		// pos =
		// "i'd like to find the cheapest flight from washington dc to atlanta",
		// ne = "O O O O O cost_relative O O city_name state_code O city_name";
		query = new QueryParser().parse(q, pos, ne);
	}

	@Test
	public void q1() {
		assertEquals(1f,
				matcher.getMatchScore("from ne-LOC to ne-LOC on ne-ORG", query)
						.getMatchingScore(), 1e-5);
	}

	@Test
	public void q2() {
		assertEquals(0, matcher.getMatchScore("dfsk dsfkjs dfskjh", query)
				.getMatchingScore(), 1e-5);
	}

	@Test
	public void q3() {
		assertEquals(
				1 - 5f / 11,
				matcher.getMatchScore(
						"from xx ne-LOC to ne-LOC the the the the on ne-ORG",
						query).getMatchingScore(), 1e-5);
	}

	@Test
	public void q4() {
		assertEquals(
				1 - 1f / 7,
				matcher.getMatchScore("the from ne-LOC to ne-LOC on ne-ORG",
						query).getMatchingScore(), 1e-5);
	}

	@Test
	public void q5() {
		assertEquals(
				1 - 2f / 7,
				matcher.getMatchScore("from ne-LOC to ne-LOC ona ne-ORG the",
						query).getMatchingScore(), 1e-5);
	}

	@Test
	public void q6() {
		assertEquals(
				1 - 1f / 7,
				matcher.getMatchScore("xxxx from ne-LOC to ne-LOC on ne-ORG",
						query).getMatchingScore(), 1e-5);
	}
	/*
	 * @Test public void q7(){ assertEquals(1, matcher.getMatchScore(
	 * "ne-cost_relative flight from ne-city_name ne-state_code", query), 1e-5);
	 * 
	 * }
	 */
}