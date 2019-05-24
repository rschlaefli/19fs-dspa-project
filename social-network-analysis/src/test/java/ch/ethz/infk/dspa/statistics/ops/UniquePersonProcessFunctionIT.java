package ch.ethz.infk.dspa.statistics.ops;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeEach;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.StatisticsOutput;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.PostActivityTestDataGenerator;
import org.junit.jupiter.api.Test;

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
	void testUniquePersonProcessFunction() throws Exception {
		this.postActivityStream
				.keyBy(PostActivity::getPostId)
				.process(new UniquePersonProcessFunction(Time.hours(1), Time.hours(12)))
				.addSink(new TestSink<>());

		this.env.execute();

		Map<Long, List<StatisticsOutput>> expectedResults = buildExpectedResults();

		Map<Long, List<StatisticsOutput>> actualResults = TestSink.getResultsInTumblingWindow(StatisticsOutput.class,
				Time.hours(1L));

		for (Map.Entry<Long, List<StatisticsOutput>> expectedResult : expectedResults.entrySet()) {
			List<StatisticsOutput> actualWindow = actualResults.get(expectedResult.getKey());
			assertNotNull(actualWindow,
					"Expected window " + expectedResult.getKey() + " not present in actual results!");

			for (StatisticsOutput statisticsOutput : expectedResult.getValue()) {
				assertTrue(actualWindow.remove(statisticsOutput),
						"Expected result " + statisticsOutput + " not present in window " + expectedResult.getKey()
								+ actualWindow);
			}
		}

		actualResults.entrySet().stream()
				.sorted(Comparator.comparingLong(Map.Entry::getKey))
				.forEach(entry -> assertTrue(entry.getValue().isEmpty(),
						String.format("More results (%s) than expected in window %d", entry.getValue().size(),
								entry.getKey())));
	}

	private Map<Long, List<StatisticsOutput>> buildExpectedResults() {
		Map<Long, List<StatisticsOutput>> expectedResults = new HashMap<>();

		// 2012-06-15 07:00:00
		expectedResults.put(1339743600000L, Arrays.asList(
				new StatisticsOutput(0L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 08:00:00
		expectedResults.put(1339747200000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 09:00:00
		expectedResults.put(1339750800000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 10:00:00
		expectedResults.put(1339754400000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 11:00:00
		expectedResults.put(1339758000000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 12:00:00
		expectedResults.put(1339761600000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 13:00:00
		expectedResults.put(1339765200000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 14:00:00
		expectedResults.put(1339768800000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 15:00:00
		expectedResults.put(1339772400000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 16:00:00
		expectedResults.put(1339776000000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 17:00:00
		expectedResults.put(1339779600000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 18:00:00
		expectedResults.put(1339783200000L, Arrays.asList(
				new StatisticsOutput(0L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 19:00:00
		expectedResults.put(1339786800000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 20:00:00
		expectedResults.put(1339790400000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 21:00:00
		expectedResults.put(1339794000000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 4L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 22:00:00
		expectedResults.put(1339797600000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 4L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-15 23:00:00
		expectedResults.put(1339801200000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 4L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-16 00:00:00
		expectedResults.put(1339804800000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-16 01:00:00
		expectedResults.put(1339808400000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-16 02:00:00
		expectedResults.put(1339812000000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 3L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-16 03:00:00
		expectedResults.put(1339815600000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-16 04:00:00
		expectedResults.put(1339819200000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(3L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-16 05:00:00
		expectedResults.put(1339822800000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(4L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-16 06:00:00
		expectedResults.put(1339826400000L, Arrays.asList(
				new StatisticsOutput(0L, 2L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(1L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-16 07:00:00
		expectedResults.put(1339830000000L, Arrays.asList(
				new StatisticsOutput(1L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT),
				new StatisticsOutput(2L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		// 2012-06-16 08:00:00
		expectedResults.put(1339833600000L, Arrays.asList(
				new StatisticsOutput(2L, 1L, StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT)));

		return expectedResults;
	}
}
