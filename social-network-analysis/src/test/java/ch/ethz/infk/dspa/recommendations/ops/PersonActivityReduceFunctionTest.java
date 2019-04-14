package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class PersonActivityReduceFunctionTest {

	@Test
	public void testPersonActivityReduceFunction() throws Exception {

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

		assertEquals("person id test", personId, reducedActivity.personId());
		assertNull("post id test", reducedActivity.postId());
		assertEquals("category count: a", 2, reducedActivity.count("a"));
		assertEquals("category count: b", 3, reducedActivity.count("b"));
		assertEquals("category count: c", 4, reducedActivity.count("c"));
		assertEquals("category count: d", 3, reducedActivity.count("d"));
		assertEquals("category count: e", 0, reducedActivity.count("e"));

	}

}
