package ch.ethz.infk.dspa.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.ops.PersonActivityReduceFunction;

public class PersonActivityReduceFunctionTest {

	@Test
	public void testReduce() throws Exception {

		PersonActivityReduceFunction activityReduce = new PersonActivityReduceFunction();

		Long personId = 1l;

		PersonActivity activity1 = new PersonActivity();
		activity1.setPersonId(personId);
		activity1.setPostId(10l);
		activity1.putCategory("a", 2);
		activity1.putCategory("b", 1);
		activity1.putCategory("c", 3);

		PersonActivity activity2 = new PersonActivity();
		activity2.setPersonId(personId);
		activity2.setPostId(12l);
		activity2.putCategory("b", 2);
		activity2.putCategory("c", 1);
		activity2.putCategory("d", 3);

		PersonActivity reducedActivity = activityReduce.reduce(activity1, activity2);

		assertEquals(personId, reducedActivity.personId(), "person id test");
		assertNull(reducedActivity.postId(), "post id test");
		assertEquals(2, reducedActivity.count("a"), "category count: a");
		assertEquals(3, reducedActivity.count("b"), "category count: b");
		assertEquals(4, reducedActivity.count("c"), "category count: c");
		assertEquals(3, reducedActivity.count("d"), "category count: d");
		assertEquals(0, reducedActivity.count("e"), "category count: e");

	}

}
