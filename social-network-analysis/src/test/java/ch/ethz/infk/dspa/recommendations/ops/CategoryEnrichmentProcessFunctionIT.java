package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Objects;

import ch.ethz.infk.dspa.recommendations.dto.Category.CategoryType;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.PersonActivityTestDataGenerator;

public class CategoryEnrichmentProcessFunctionIT {

	// TODO adjust test (include tagclass enrichment, maybe actually compare category map)

	private StreamExecutionEnvironment env;
	private DataStream<PersonActivity> personActivityStream;

	@BeforeEach
	void setup() throws IOException {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		this.personActivityStream = new PersonActivityTestDataGenerator().generate(this.env,
				"src/test/java/resources/recommendations/streams/person_activity_stream_enrichment.csv",
				Time.hours(1));

		TestSink.reset();
	}

	@Test
	void testCategoryEnrichmentProcessFunction() throws Exception {

		this.personActivityStream.keyBy(PersonActivity::postId).process(new CategoryEnrichmentProcessFunction(
				"src/test/java/resources/recommendations/relations/forum_hasTag_tag.csv",
				"src/test/java/resources/recommendations/relations/place_isPartOf_place.csv",
				"src/test/java/resources/recommendations/relations/tag_hasType_tagclass.csv",
				"src/test/java/resources/recommendations/relations/tagclass_isSubclassOf_tagclass.csv"))
				.addSink(new TestSink<>());

		this.env.execute();

		List<ActivityTuple> expectedResults = new ArrayList<>(Arrays.asList(
				new ActivityTuple(1L, 1L, 1L, 1L),
				new ActivityTuple(2L, 1L, 1L, 2L),
				new ActivityTuple(4L, 1L, 1L, null),
				new ActivityTuple(3L, 2L, 1L, 2L),
				new ActivityTuple(5L, 3L, 2L, 1L),
				new ActivityTuple(9L, 2L, 1L, 1L),
				new ActivityTuple(11L, 2L, 1L, null),
				new ActivityTuple(13L, 3L, 2L, null),
				new ActivityTuple(12L, 4L, 3L, 3L),
				new ActivityTuple(16L, 3L, 2L, 3L),
				new ActivityTuple(14L, 4L, 3L, null),
				new ActivityTuple(17L, 4L, 3L, 2L),
				new ActivityTuple(18L, 2L, 1L, null),
				new ActivityTuple(21L, 3L, 2L, null),
				new ActivityTuple(22L, 3L, 2L, 1L),
				new ActivityTuple(23L, 5L, 4L, 2L),
				new ActivityTuple(25L, 5L, 4L, 2L),
				new ActivityTuple(27L, 6L, 2L, 4L),
				new ActivityTuple(38L, 5L, 4L, null),
				new ActivityTuple(39L, 2L, 1L, 4L),
				new ActivityTuple(37L, 3L, 2L, null),
				new ActivityTuple(33L, 1L, 1L, 3L)));

		List<PersonActivity> actualResults = TestSink.getResults(PersonActivity.class);

		for (PersonActivity actualActivity : actualResults) {
			ActivityTuple actualActivityTuple = new ActivityTuple(actualActivity);
			System.out.println(actualActivityTuple);

			assertTrue(
					expectedResults.remove(actualActivityTuple),
					"Actual result " + actualActivityTuple + " not expected!");
		}

		for (ActivityTuple expectedActivityTuple : expectedResults) {
			fail(expectedActivityTuple + " not found!");
		}
	}

	private class ActivityTuple {
		Long personId;
		Long postId;
		Long forumId;
		Long placeId;

		ActivityTuple(PersonActivity personActivity) {
			this.personId = personActivity.personId();
			this.postId = personActivity.postId();
			this.forumId = personActivity.extractLongIdFromKeySet(CategoryType.FORUM);
			this.placeId = personActivity.extractLongIdFromKeySet(CategoryType.PLACE);
		}

		ActivityTuple(Long personId, Long postId, Long forumId, Long placeId) {
			this.personId = personId;
			this.postId = postId;
			this.forumId = forumId;
			this.placeId = placeId;
		}

		@Override
		public String toString() {
			return "ActivityTuple{" +
					"personId=" + personId +
					", postId=" + postId +
					", forumId=" + forumId +
					", placeId=" + placeId +
					'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ActivityTuple that = (ActivityTuple) o;
			return Objects.equal(personId, that.personId) &&
					Objects.equal(postId, that.postId) &&
					Objects.equal(forumId, that.forumId) &&
					Objects.equal(placeId, that.placeId);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(personId, postId, forumId, placeId);
		}
	}
}
