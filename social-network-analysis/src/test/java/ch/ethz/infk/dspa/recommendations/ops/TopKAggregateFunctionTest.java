package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;

public class TopKAggregateFunctionTest {

	@Test
	public void testTopKAggregateFunctionTest() {

		int k = 10;

		Long person1Id = 1l;

		TopKAggregateFunction function = new TopKAggregateFunction(k);

		// Build 1st Accumulator

		PriorityQueue<PersonSimilarity> accumulator1 = function.createAccumulator();

		Double[] sim1 = new Double[] { 1.0, 20.0, 0.0, 13.0, 20.0, 21.0, 2.0, 1.0, 0.0, 3.0, 3.0, 4.0, 5.0, 6.0, 7.0,
				0.0 };
		for (int i = 0; i < sim1.length; i++) {
			accumulator1 = function.add(new PersonSimilarity(person1Id, (long) i).withSimilarity(sim1[i]),
					accumulator1);
		}

		List<PersonSimilarity> results1 = function.getResult(accumulator1);

		Arrays.sort(sim1, Collections.reverseOrder()); // sort in descending order

		// Check 1st Results
		assertEquals(Integer.min(k, sim1.length), results1.size(), "Results1 wrong size");
		for (int i = 0; i < Integer.min(k, sim1.length); i++) {
			assertEquals(sim1[i], results1.get(i).similarity(), "Results1 expected other at index: " + i);
		}

		// Build 2nd Accumulator

		PriorityQueue<PersonSimilarity> accumulator2 = function.createAccumulator();
		Double[] sim2 = new Double[] { 0.0, 0.0, 1.0, 2.0, 3.0, 50.0, 23.0 };
		for (int i = 0; i < sim2.length; i++) {
			accumulator2 = function.add(new PersonSimilarity(person1Id, (long) i).withSimilarity(sim2[i]),
					accumulator2);
		}

		List<PersonSimilarity> results2 = function.getResult(accumulator2);

		Arrays.sort(sim2, Collections.reverseOrder()); // sort in descending order

		// Check 2nd Results
		assertEquals(Integer.min(k, sim2.length), results2.size(), "Results2 wrong size");

		for (int i = 0; i < Integer.min(k, sim2.length); i++) {
			assertEquals(sim2[i], results2.get(i).similarity(), "Results2 expected other at index: " + i);
		}

		// Build Combined Accumulator

		PriorityQueue<PersonSimilarity> accumulator = function.merge(accumulator1, accumulator2);
		List<PersonSimilarity> results = function.getResult(accumulator);

		Double[] sim = ArrayUtils.addAll(sim1, sim2);
		Arrays.sort(sim, Collections.reverseOrder()); // sort in descending order

		// Check Combined Results
		assertEquals(Integer.min(k, sim2.length), results2.size(), "Results2 wrong size");
		for (int i = 0; i < k; i++) {
			assertEquals(sim[i], results.get(i).similarity(), "Results2 expected other at index: " + i);
		}

		// check that other fields are set
		for (PersonSimilarity similarity : results) {
			assertEquals(person1Id, similarity.person1Id(), "Person1Id wrong");
			assertNotNull(similarity.person2Id(), "Person2Id not set");
		}

	}

}
