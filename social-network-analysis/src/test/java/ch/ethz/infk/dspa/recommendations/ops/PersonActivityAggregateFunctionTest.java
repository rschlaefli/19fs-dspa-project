package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class PersonActivityAggregateFunctionTest {

	@Test
	public void testPersonActivityReduceFunction() throws Exception {

		PersonActivityAggregateFunction activityAggregate = new PersonActivityAggregateFunction();

		Long personId = 1l;

		PersonActivity activity1 = new PersonActivity();
		activity1.setPersonId(personId);
		activity1.setPostId(10l);
		Map<String, Integer> categoryMap1 = new HashMap<>();
		categoryMap1.put("a", 2);
		categoryMap1.put("b", 1);
		categoryMap1.put("c", 3);
		activity1.setCategoryMap(categoryMap1);

		PersonActivity activity2 = new PersonActivity();
		activity2.setPersonId(personId);
		activity2.setPostId(12l);

		Map<String, Integer> categoryMap2 = new HashMap<>();
		categoryMap2.put("b", 2);
		categoryMap2.put("c", 1);
		categoryMap2.put("d", 3);
		activity2.setCategoryMap(categoryMap2);

		PersonActivity accumulator = activityAggregate.createAccumulator();

		accumulator = activityAggregate.add(activity1, accumulator);
		accumulator = activityAggregate.add(activity2, accumulator);
		PersonActivity reducedActivity = activityAggregate.getResult(accumulator);

		assertEquals(personId, reducedActivity.getPersonId(), "person id test");
		assertNull(reducedActivity.getPostId(), "post id test");
		assertEquals(Integer.valueOf(2), reducedActivity.getCategoryMap().get("a"), "category count: a");
		assertEquals(Integer.valueOf(3), reducedActivity.getCategoryMap().get("b"), "category count: b");
		assertEquals(Integer.valueOf(4), reducedActivity.getCategoryMap().get("c"), "category count: c");
		assertEquals(Integer.valueOf(3), reducedActivity.getCategoryMap().get("d"), "category count: d");

	}

}
