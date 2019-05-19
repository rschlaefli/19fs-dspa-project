package ch.ethz.infk.dspa.anomalies.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import ch.ethz.infk.dspa.anomalies.dto.EventStatistics;
import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.Feature.FeatureId;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.FeatureStatisticsTestDataGenerator;

public class EnsembleProcessFunctionTest extends AbstractTestBase {

	private StreamExecutionEnvironment env;
	private DataStream<FeatureStatistics> featureStatsStream;

	private ImmutableMap<FeatureId, Double> thresholds;

	@BeforeEach
	public void setupEach() throws IOException {
		TestSink.reset();

		// build test data
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		String testFile = "src/test/java/resources/anomalies/streams/featurestats_event_stream.csv";
		Time maxOutOfOrderness = Time.hours(1);

		FeatureStatisticsTestDataGenerator testDataGenerator = new FeatureStatisticsTestDataGenerator();
		featureStatsStream = testDataGenerator.generate(env, testFile, maxOutOfOrderness);

		thresholds = new ImmutableMap.Builder<Feature.FeatureId, Double>()
				.put(FeatureId.TIMESPAN, 1.0)
				.put(FeatureId.CONTENTS_SHORT, 2.0)
				.put(FeatureId.CONTENTS_MEDIUM, 1.0)
				.put(FeatureId.CONTENTS_LONG, 1.0)
				.put(FeatureId.TAG_COUNT, 1.0)
				.put(FeatureId.NEW_USER_LIKES, 1.0)
				.put(FeatureId.INTERACTIONS_RATIO, 2.0)
				.build();
	}

	@Test
	public void testEnsembleProcessFunction() throws Exception {
		featureStatsStream
				.keyBy(FeatureStatistics::getEventGUID)
				.process(new EnsembleProcessFunction(thresholds))
				.addSink(new TestSink<EventStatistics>());

		this.env.execute();

		List<EventStatistics> results = TestSink.getResults(EventStatistics.class);

		assertEquals(3, results.size(), "Unexpected result size");

		// check post result
		EventStatistics postResult = results.stream().filter(x -> x.getEventGUID().startsWith("POST")).findAny().get();
		checkPostResults(postResult);

		// check comment result
		EventStatistics commentResult = results.stream().filter(x -> x.getEventGUID().startsWith("COMMENT")).findAny()
				.get();
		checkCommentResults(commentResult);

		// check like result
		EventStatistics likeResult = results.stream().filter(x -> x.getEventGUID().startsWith("LIKE")).findAny().get();
		checkLikeResults(likeResult);

	}

	private void checkPostResults(EventStatistics postResult) {
		Set<FeatureId> expectedFraudulentFeatures = new HashSet<>(
				Arrays.asList(FeatureId.TAG_COUNT));
		Set<FeatureId> expectedNonFraudulentFeatures = new HashSet<>(
				Arrays.asList(FeatureId.TIMESPAN, FeatureId.CONTENTS_MEDIUM));

		Set<FeatureId> actualFraudulentFeatures = postResult.getVotesFraudulent().stream()
				.map(Feature::getFeatureId)
				.collect(Collectors.toSet());

		Set<FeatureId> actualNonFraudulentFeatures = postResult.getVotesNonFraudulent().stream()
				.map(Feature::getFeatureId)
				.collect(Collectors.toSet());

		assertEquals(expectedFraudulentFeatures, actualFraudulentFeatures, "post: fraudulent features");

		assertEquals(expectedNonFraudulentFeatures, actualNonFraudulentFeatures,
				"post: non-fraudulent features");

	}

	private void checkCommentResults(EventStatistics commentResult) {
		Set<FeatureId> expectedFraudulentFeatures = new HashSet<>(
				Arrays.asList(FeatureId.CONTENTS_SHORT));
		Set<FeatureId> expectedNonFraudulentFeatures = new HashSet<>(
				Arrays.asList(FeatureId.TIMESPAN));

		Set<FeatureId> actualFraudulentFeatures = commentResult.getVotesFraudulent().stream()
				.map(Feature::getFeatureId)
				.collect(Collectors.toSet());

		assertEquals(expectedFraudulentFeatures, actualFraudulentFeatures, "comment: fraudulent features");

		Set<FeatureId> actualNonFraudulentFeatures = commentResult.getVotesNonFraudulent().stream()
				.map(Feature::getFeatureId)
				.collect(Collectors.toSet());
		assertEquals(expectedNonFraudulentFeatures, actualNonFraudulentFeatures,
				"comment: non-fraudulent features");

	}

	private void checkLikeResults(EventStatistics likeResult) {
		Set<FeatureId> expectedFraudulentFeatures = new HashSet<>(
				Arrays.asList(FeatureId.TIMESPAN, FeatureId.INTERACTIONS_RATIO));
		Set<FeatureId> expectedNonFraudulentFeatures = new HashSet<>(
				Arrays.asList(FeatureId.NEW_USER_LIKES));

		Set<FeatureId> actualFraudulentFeatures = likeResult.getVotesFraudulent().stream()
				.map(Feature::getFeatureId)
				.collect(Collectors.toSet());

		assertEquals(expectedFraudulentFeatures, actualFraudulentFeatures, "like: fraudulent features");

		Set<FeatureId> actualNonFraudulentFeatures = likeResult.getVotesNonFraudulent().stream()
				.map(Feature::getFeatureId)
				.collect(Collectors.toSet());
		assertEquals(expectedNonFraudulentFeatures, actualNonFraudulentFeatures,
				"like: non-fraudulent features");

	}

}
