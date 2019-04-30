package ch.ethz.infk.dspa.statistics.ops;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;
import ch.ethz.infk.dspa.stream.testdata.PostActivityTestDataGenerator;

public class TypeCountAggregateFunctionTest {

	private static List<PostActivity> postActivities;

	@BeforeAll
	public static void setup() throws IOException {
		postActivities = new PostActivityTestDataGenerator()
				.generate("./src/test/java/resources/post_activity_event_stream.csv");
	}

	@Test
	public void testPostCountAggregateFunction() {
		TypeCountAggregateFunction function = new TypeCountAggregateFunction(ActivityType.POST);

		long nPostsExpected = postActivities.stream().filter(act -> act.getType() == ActivityType.POST).count();
		long nPostsActual = calculateTypeCountAggregateResult(function);

		assert (nPostsExpected > 0);
		assertEquals("TypeContAggregateFunction failed for Posts", nPostsExpected, nPostsActual);

	}

	@Test
	public void testCommentCountAggregateFunction() {
		TypeCountAggregateFunction function = new TypeCountAggregateFunction(ActivityType.COMMENT);
		long nCommentsExpected = postActivities.stream().filter(act -> act.getType() == ActivityType.COMMENT).count();
		long nCommentsActual = calculateTypeCountAggregateResult(function);

		assert (nCommentsExpected > 0);
		assertEquals("TypeContAggregateFunction failed for Comments", nCommentsExpected, nCommentsActual);
	}

	@Test
	public void testReplyCountAggregateFunction() {
		TypeCountAggregateFunction function = new TypeCountAggregateFunction(ActivityType.REPLY);
		long nRepliesExpected = postActivities.stream().filter(act -> act.getType() == ActivityType.REPLY).count();
		long nRepliesActual = calculateTypeCountAggregateResult(function);

		assert (nRepliesExpected > 0);
		assertEquals("TypeContAggregateFunction failed for Replies", nRepliesExpected, nRepliesActual);
	}

	private Long calculateTypeCountAggregateResult(TypeCountAggregateFunction function) {
		Long accumulator = function.createAccumulator();

		for (PostActivity activity : postActivities) {
			accumulator = function.add(activity, accumulator);
		}

		return function.getResult(accumulator);
	}

}
