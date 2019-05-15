package ch.ethz.infk.dspa.anomalies.ops.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.Feature.FeatureId;
import ch.ethz.infk.dspa.anomalies.ops.features.NewUserInteractionFeatureProcessFunction;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.AbstractTestDataGenerator.TestDataPair;
import ch.ethz.infk.dspa.stream.testdata.LikeTestDataGenerator;

public class NewUserInteractionFeatureProcessFunctionIT extends AbstractTestBase {

	private StreamExecutionEnvironment env;
	private DataStream<Feature> likeFeatureStream;

	private List<Like> likes;

	@BeforeEach
	public void setup() throws IOException {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		LikeTestDataGenerator generator = new LikeTestDataGenerator();

		likeFeatureStream = generator.generate(env,
				"src/test/java/resources/anomalies/streams/like_stream_feature_newuser.csv", Time.hours(1))
				.map(Feature::of);

		likes = generator.getTestData().stream().map(TestDataPair::getElement).collect(Collectors.toList());

		TestSink.reset();
	}

	@Test
	public void testNewUserInteractionFeatureProcessFunction() throws Exception {
		Time newUserThreshold = Time.hours(24);
		String personRelationFile = "src/test/java/resources/anomalies/relations/person_feature_newuser.csv";

		new NewUserInteractionFeatureProcessFunction(newUserThreshold, personRelationFile)
				.applyTo(likeFeatureStream).addSink(new TestSink<Feature>());

		this.env.execute();

		List<Feature> expectedResults = buildExpectedResults(likes);

		Map<Long, List<Feature>> timestampedResults = TestSink.getResultsTimestamped(Feature.class);
		List<Feature> results = TestSink.getResults(Feature.class).stream()
				.sorted(Comparator.comparing(feature -> feature.getLike().getCreationDate()))
				.collect(Collectors.toList());

		assertEquals(expectedResults.size(), results.size(), "Different Number of Results");
		for (int i = 0; i < results.size(); i++) {
			Feature result = results.get(i);
			Feature expectedResult = expectedResults.get(i);

			// check correct result
			assertEquals(expectedResult.getFeatureId(), result.getFeatureId(), "Unexpected Feature Id");
			assertEquals(expectedResult.getFeatureValue(), result.getFeatureValue(), "Unexpected Feature Value");

			// check correct timestamp
			Long timestamp = result.getLike().getCreationDate().getMillis();
			timestampedResults.get(timestamp).contains(result);
		}
	}

	private List<Feature> buildExpectedResults(List<Like> likes) {
		// expected results manually calculated

		List<Feature> expectedFeatures = new ArrayList<>();

		// like of old user
		expectedFeatures
				.add(Feature.of(likes.get(0)).withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(0.0 / 1));

		// like of other post with old user
		expectedFeatures
				.add(Feature.of(likes.get(1)).withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(0.0 / 1));

		// like of new user
		expectedFeatures
				.add(Feature.of(likes.get(2)).withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(1.0 / 2));

		// like of new user
		expectedFeatures
				.add(Feature.of(likes.get(3)).withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(2.0 / 3));

		// like of new user
		expectedFeatures
				.add(Feature.of(likes.get(4)).withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(3.0 / 4));

		// like of old user
		expectedFeatures
				.add(Feature.of(likes.get(5)).withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(3.0 / 5));

		// like of old user
		expectedFeatures
				.add(Feature.of(likes.get(6)).withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(3.0 / 6));

		// like of new user
		expectedFeatures
				.add(Feature.of(likes.get(7)).withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(4.0 / 7));

		// like of new user
		expectedFeatures
				.add(Feature.of(likes.get(8)).withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(5.0 / 8));

		return expectedFeatures;
	}

}
