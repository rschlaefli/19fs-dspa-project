package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.PersonActivityTestDataGenerator;

public class PersonOutputSelectorProcessFunctionIT {

	private StreamExecutionEnvironment env;
	private DataStream<PersonActivity> personActivityStream;

	@BeforeEach
	public void setup() throws IOException {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		personActivityStream = new PersonActivityTestDataGenerator().generate(env,
				"src/test/java/resources/recommendations/streams/person_activity_stream_diverse.csv", Time.hours(1));

		TestSink.reset();
	}

	@Test
	public void testPersonOutputSelectorProcessFunction() throws Exception {
		// TODO [rsc]: evaluate window-timestamp assignments with additional process function (activity, ts)

		personActivityStream
				.process(new PersonOutputSelectorProcessFunction(4, Time.hours(1)))
				.setParallelism(1)
				.getSideOutput(PersonOutputSelectorProcessFunction.SELECTED)
				.addSink(new TestSink<>());

		env.execute();

		List<PersonActivity> selectedActivities = TestSink.getResults(PersonActivity.class);

		assertEquals(4, selectedActivities.stream()
				.filter(activity -> activity.personId() < 10)
				.count(),
				"Wrong number of selected persons for first window!");

		assertEquals(4, selectedActivities.stream()
				.filter(activity -> activity.personId() >= 10 && activity.personId() < 20)
				.count(),
				"Wrong number of selected persons for second window!");

		assertEquals(4, selectedActivities.stream()
				.filter(activity -> activity.personId() >= 20 && activity.personId() < 30)
				.count(),
				"Wrong number of selected persons for third window!");

		assertEquals(4, selectedActivities.stream()
				.filter(activity -> activity.personId() >= 30 && activity.personId() < 40)
				.count(),
				"Wrong number of selected persons for fourth window!");
		assertEquals(Arrays.asList(33L, 37L, 38L, 39L),
				selectedActivities.stream()
						.map(PersonActivity::personId)
						.filter(personId -> personId >= 30 && personId < 40)
						.sorted()
						.collect(Collectors.toList()),
				"Wrong persons selected for fourth window!");
	}
}
