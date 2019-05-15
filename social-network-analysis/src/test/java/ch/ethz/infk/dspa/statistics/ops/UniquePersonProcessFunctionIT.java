package ch.ethz.infk.dspa.statistics.ops;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import ch.ethz.infk.dspa.statistics.dto.StatisticsOutput;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.PostActivityTestDataGenerator;

public class UniquePersonProcessFunctionIT {

	private StreamExecutionEnvironment env;
	private DataStream<PostActivity> postActivityStream;

	@BeforeEach
	void setup() throws IOException {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		this.postActivityStream = new PostActivityTestDataGenerator().generate(this.env,
				"src/test/java/resources/statistics/streams/post_activity_stream.csv", Time.hours(1));

		TestSink.reset();
	}

	@Test
	void testUniquePersonProcessFunction() {
		this.postActivityStream
				.keyBy(PostActivity::getPostId)
				.process(new UniquePersonProcessFunction(Time.hours(1), Time.hours(12)))
				.addSink(new TestSink<>());

		try {
			this.env.execute();
		} catch (Exception e) {
			fail("Failure in Flink Topology");
		}

		List<StatisticsOutput> expectedResults = Arrays.asList(
				new StatisticsOutput(1339747200000L, 0L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339750800000L, 0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339754400000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339758000000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339761600000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339765200000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339768800000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339772400000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339776000000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339779600000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339783200000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339786800000L, 0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339790400000L, 0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),

				new StatisticsOutput(1339758000000L, 1L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339761600000L, 1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339765200000L, 1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339768800000L, 1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339772400000L, 1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339776000000L, 1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339779600000L, 1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339783200000L, 1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339786800000L, 1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339790400000L, 1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),

				new StatisticsOutput(1339765200000L, 2L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339768800000L, 2L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339772400000L, 2L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339776000000L, 2L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339779600000L, 2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339783200000L, 2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339786800000L, 2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339790400000L, 2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),

				new StatisticsOutput(1339783200000L, 3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339786800000L, 3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339790400000L, 3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),

				new StatisticsOutput(1339786800000L, 4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1339790400000L, 4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT));

		List<StatisticsOutput> actualResults = TestSink.getResults(StatisticsOutput.class);

		for (StatisticsOutput expectedResult : expectedResults) {
			assertTrue(actualResults.remove(expectedResult), "Expected result " + expectedResult + " not present!");
		}

		assertTrue(actualResults.isEmpty(), "Received more results than expected!");
	}
}
