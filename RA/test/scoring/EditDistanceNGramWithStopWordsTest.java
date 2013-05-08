package scoring;

import static org.junit.Assert.*;

import org.junit.Test;

import rankinggraph.QueryInfo;
import rankinggraph.QueryParser;
import rankinggraph.scoring.EditDistanceNGramWithStopWordsMatcher;
import rankinggraph.scoring.MatchingRecord;

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
		MatchingRecord match = matcher.getMatchScore(
				"from ne-LOC to ne-LOC on ne-ORG", query);
		System.out.println(match.getLinks());
		assertEquals(1f, match.getMatchingScore(), 1e-5);
	}

	@Test
	public void q2() {
		MatchingRecord match = matcher.getMatchScore("dfsk dsfkjs dfskjh",
				query);
		System.out.println(match.getLinks());
		assertEquals(1 - 2f / 3, match.getMatchingScore(), 1e-5);
	}

	@Test
	public void q3() {
		MatchingRecord match = matcher.getMatchScore(
				"from xxx ne-LOC to ne-LOC the the the the on ne-ORG", query);
		System.out.println(match.getLinks());
		assertEquals(1 - 3f / 11, match.getMatchingScore(), 1e-5);
	}

	@Test
	public void q4() {
		MatchingRecord match = matcher.getMatchScore(
				"the from ne-LOC to ne-LOC on ne-ORG", query);
		System.out.println(match.getLinks());
		assertEquals(1 - 0.5f / 7, match.getMatchingScore(), 1e-5);
	}

	@Test
	public void q5() {
		MatchingRecord match = matcher.getMatchScore(
				"from ne-LOC to ne-LOC ona ne-ORG the", query);
		System.out.println(match.getLinks());
		assertEquals(1 - 1f / 7, match.getMatchingScore(), 1e-5);
	}

	@Test
	public void q6() {
		MatchingRecord match = matcher.getMatchScore(
				"xxxx from ne-LOC to ne-LOC on ne-ORG", query);
		System.out.println(match.getLinks());
		assertEquals(1 - 1f / 7, match.getMatchingScore(), 1e-5);
	}

}
