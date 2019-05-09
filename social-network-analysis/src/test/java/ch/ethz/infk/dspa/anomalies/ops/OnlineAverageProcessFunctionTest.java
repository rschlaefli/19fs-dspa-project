package ch.ethz.infk.dspa.anomalies.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.math.Stats;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.AbstractTestDataGenerator.TestDataPair;
import ch.ethz.infk.dspa.stream.testdata.FeatureTestDataGenerator;

public class OnlineAverageProcessFunctionTest extends AbstractTestBase {

	private StreamExecutionEnvironment env;
	private DataStream<Feature> featureStream;
	private List<TestDataPair<Feature>> testData;

	@BeforeEach
	public void setupEach() throws IOException {
		TestSink.reset();

		// build test data
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		String testFile = "./../data/test/01_test/feature_event_stream.csv";
		Time maxOutOfOrderness = Time.hours(1);

		FeatureTestDataGenerator testDataGenerator = new FeatureTestDataGenerator();
		featureStream = testDataGenerator.generate(env, testFile, maxOutOfOrderness);
		testData = testDataGenerator.getTestData();
	}

	@Test
	public void testOnlineAverageProcessFunction() throws Exception {

		// build expected results
		HashMap<String, StatsContainer> expectedResults = buildExpectedResults(testData);

		// build dataflow pipeline
		featureStream
				.keyBy(Feature::getFeatureId)
				.process(new OnlineAverageProcessFunction())
				.returns(FeatureStatistics.class)
				.addSink(new TestSink<FeatureStatistics>());

		// execute dataflow pipeline
		env.execute();

		// check results
		List<FeatureStatistics> results = TestSink.getResults(FeatureStatistics.class);

		for (FeatureStatistics result : results) {

			String eventId = result.getEventGUID();
			StatsContainer container = expectedResults.remove(eventId);

			assertNotNull(container, "Unexpected EventId");
			assertEquals(container.mean, result.getMean(), 0.00001, "Unexpected Mean");
			assertEquals(container.stdDev, result.getStdDev(), 0.00001, "Unexpected Std Dev");

//			System.out.println(String.format("Mean=%f   StdDev=%f ", result.mean, result.stdDev));
//			System.out.println(String.format("ExpectedMean=%f   ExpectedStdDev=%f ", container.mean, container.stdDev));
//			System.out.println(result.feature);

		}

		assertEquals(1, expectedResults.size(), "Additional Result Expected (end marker should still be there)");

	}

	private HashMap<String, StatsContainer> buildExpectedResults(List<TestDataPair<Feature>> testData) {
		HashMap<Long, Double> meanMap = new HashMap<>();
		HashMap<Long, Double> stdDevMap = new HashMap<>();

		HashMap<String, StatsContainer> expectedResults = new HashMap<>();

		// collect all timestamps in set (remove duplicates)
		Set<Long> timestamps = testData.stream().map(x -> x.getTimestamp().getMillis()).collect(Collectors.toSet());

		// calculate for every timestamp the mean and average (from events up to and including timestamp)
		for (Long timestamp : timestamps) {

			List<Double> featureValues = testData.stream()
					.filter(x -> x.getTimestamp().getMillis() <= timestamp)
					.map(TestDataPair::getElement)
					.map(Feature::getFeatureValue)
					.collect(Collectors.toList());

			Stats stats = Stats.of(featureValues);
			meanMap.put(timestamp, stats.mean());
			stdDevMap.put(timestamp, stats.populationStandardDeviation());

		}

		// Build expected result Map
		for (TestDataPair<Feature> pair : testData) {
			String eventId = pair.getElement().getGUID();
			long timestamp = pair.getTimestamp().getMillis();

			double mean = meanMap.get(timestamp);
			double stdDev = stdDevMap.get(timestamp);

			expectedResults.put(eventId, new StatsContainer(mean, stdDev));
		}

		return expectedResults;
	}

	private static class StatsContainer {
		double mean;
		double stdDev;

		public StatsContainer(double mean, double stdDev) {
			this.mean = mean;
			this.stdDev = stdDev;
		}

	}

}
