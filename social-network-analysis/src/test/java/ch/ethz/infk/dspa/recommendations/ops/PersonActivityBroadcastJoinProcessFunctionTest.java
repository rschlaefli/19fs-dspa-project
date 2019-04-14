package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.PersonActivityTestDataGenerator;

public class PersonActivityBroadcastJoinProcessFunctionTest extends AbstractTestBase {

	@Test
	public void testPersonActivityBroadcastJoinProcessFunction() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String testFile = "./../data/test/01_test/reduced_person_activity_event_stream.csv";
		Time maxOutOfOrderness = Time.hours(1);

		DataStream<PersonActivity> activityStream = new PersonActivityTestDataGenerator().generate(env, testFile,
				maxOutOfOrderness);

		BroadcastStream<PersonActivity> selectedBroadcastStream = new PersonActivityTestDataGenerator()
				.generate(env, testFile, maxOutOfOrderness)
				.filter(activity -> activity.personId() == 1 || activity.personId() == 2)
				.broadcast(PersonActivityBroadcastJoinProcessFunction.SELECTED_PERSON_STATE_DESCRIPTOR);

		activityStream
				.connect(selectedBroadcastStream)
				.process(new PersonActivityBroadcastJoinProcessFunction(10, Time.hours(1)))
				.returns(new TypeHint<PersonSimilarity>() {
				})
				.addSink(new TestSink<PersonSimilarity>());

		env.execute();

		List<PersonSimilarity> results = TestSink.getResults(PersonSimilarity.class);
		List<PersonSimilarity> expectedResults = buildExpectedResults();

		for (PersonSimilarity x : results) {
			System.out.println(x);
		}

		for (PersonSimilarity x : expectedResults) {
			assertTrue(results.remove(x), "Expected Similarity not present: " + x);
		}

		assertTrue(results.isEmpty(), "Unexpected Result");

	}

	public List<PersonSimilarity> buildExpectedResults() {
		List<PersonSimilarity> expected = new ArrayList<PersonSimilarity>();

		// First Slot
		expected.add(new PersonSimilarity(1l, 1l).withSimilarity(5.0));
		expected.add(new PersonSimilarity(1l, 2l).withSimilarity(1.0));
		expected.add(new PersonSimilarity(1l, 3l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(1l, 4l).withSimilarity(3.0));
		expected.add(new PersonSimilarity(1l, 5l).withSimilarity(3.0));
		expected.add(new PersonSimilarity(1l, 6l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(1l, 7l).withSimilarity(2.0));
		expected.add(new PersonSimilarity(1l, 8l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(1l, 9l).withSimilarity(1.0));
		expected.add(new PersonSimilarity(1l, 10l).withSimilarity(2.0));

		expected.add(new PersonSimilarity(2l, 1l).withSimilarity(1.0));
		expected.add(new PersonSimilarity(2l, 2l).withSimilarity(10.0));
		expected.add(new PersonSimilarity(2l, 3l).withSimilarity(12.0));
		expected.add(new PersonSimilarity(2l, 4l).withSimilarity(4.0));
		expected.add(new PersonSimilarity(2l, 5l).withSimilarity(3.0));
		expected.add(new PersonSimilarity(2l, 6l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 7l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 8l).withSimilarity(3.0));
		expected.add(new PersonSimilarity(2l, 9l).withSimilarity(1.0));
		expected.add(new PersonSimilarity(2l, 10l).withSimilarity(6.0));

		// Second Slot
		expected.add(new PersonSimilarity(1l, 1l).withSimilarity(1.0));
		expected.add(new PersonSimilarity(1l, 2l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(1l, 3l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(1l, 4l).withSimilarity(2.0));
		expected.add(new PersonSimilarity(1l, 5l).withSimilarity(1.0));
		expected.add(new PersonSimilarity(1l, 6l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(1l, 7l).withSimilarity(2.0));
		expected.add(new PersonSimilarity(1l, 8l).withSimilarity(1.0));
		expected.add(new PersonSimilarity(1l, 11l).withSimilarity(2.0));
		expected.add(new PersonSimilarity(1l, 12l).withSimilarity(3.0));
		expected.add(new PersonSimilarity(1l, 13l).withSimilarity(2.0));

		expected.add(new PersonSimilarity(2l, 1l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 2l).withSimilarity(1.0));
		expected.add(new PersonSimilarity(2l, 3l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 4l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 5l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 6l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 7l).withSimilarity(2.0));
		expected.add(new PersonSimilarity(2l, 8l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 11l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 12l).withSimilarity(0.0));
		expected.add(new PersonSimilarity(2l, 13l).withSimilarity(0.0));

		return expected;

	}
}
