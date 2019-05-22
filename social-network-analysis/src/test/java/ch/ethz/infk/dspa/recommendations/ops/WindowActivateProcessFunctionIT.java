package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.PersonActivityTestDataGenerator;

public class WindowActivateProcessFunctionIT extends AbstractTestBase {

	private StreamExecutionEnvironment env;
	private DataStream<PersonActivity> personActivityStream;

	private Set<Long> testDataTimestamps;

	@BeforeEach
	void setup() throws IOException {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		PersonActivityTestDataGenerator generator = new PersonActivityTestDataGenerator();
		this.personActivityStream = generator.generate(this.env,
				"src/test/java/resources/recommendations/streams/person_activity_stream_enrichment.csv",
				Time.hours(1));

		this.testDataTimestamps = generator.getTestData().stream().map(x -> x.getTimestamp().getMillis())
				.collect(Collectors.toSet());

		TestSink.reset();
	}

	@Test
	public void testWindowActivateProcessFunctionIT() throws Exception {
		int parallelism = 4;
		env.setParallelism(parallelism);

		Time windowSize = Time.hours(4);

		personActivityStream.process(new WindowActivateProcessFunction(windowSize))
				.addSink(new TestSink<Tuple0>());

		env.execute();

		Map<Long, List<Tuple0>> results = TestSink.getResultsInTumblingWindow(Tuple0.class, windowSize);

		Set<Long> expectedTimestamps = buildExpectedResults(testDataTimestamps, windowSize);
		assertEquals(expectedTimestamps, results.keySet(), "unexpected timestamps");

		for (List<Tuple0> outputs : results.values()) {
			assertTrue(outputs.size() <= parallelism, "more outputs than expected");
		}

	}

	private Set<Long> buildExpectedResults(Set<Long> testDataTimestamps, Time windowSize) {
		return testDataTimestamps.stream()
				.map(x -> TimeWindow.getWindowStartWithOffset(x, 0, windowSize.toMilliseconds()))
				.collect(Collectors.toSet());

	}

}
