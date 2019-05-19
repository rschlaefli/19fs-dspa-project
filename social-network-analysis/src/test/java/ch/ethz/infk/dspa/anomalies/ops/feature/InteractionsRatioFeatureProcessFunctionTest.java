package ch.ethz.infk.dspa.anomalies.ops.feature;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.ethz.infk.dspa.anomalies.ops.features.InteractionsRatioFeatureProcessFunction;
import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.StatisticsOutput;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.AbstractTestDataGenerator.TestDataPair;
import ch.ethz.infk.dspa.stream.testdata.LikeTestDataGenerator;

import static org.junit.jupiter.api.Assertions.*;

public class InteractionsRatioFeatureProcessFunctionTest extends AbstractTestBase {

	private StreamExecutionEnvironment env;

	private CommentTestDataGenerator commentTestDataGenerator;
	private LikeTestDataGenerator likeTestDataGenerator;
	private DataStream<Feature> commentFeatureStream;
	private DataStream<Feature> likeFeatureStream;

	@BeforeEach
	public void setup() throws IOException {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		commentTestDataGenerator = new CommentTestDataGenerator();
		likeTestDataGenerator = new LikeTestDataGenerator();

		commentFeatureStream = commentTestDataGenerator
				.generate(env, "src/test/java/resources/anomalies/streams/comment_stream_feature_interactions.csv",
						Time.hours(1))
				.map(Feature::of);
		likeFeatureStream = likeTestDataGenerator
				.generate(env, "src/test/java/resources/anomalies/streams/like_stream_feature_interactions.csv",
						Time.hours(1))
				.map(Feature::of);

		TestSink.reset();
	}

	@Test
	public void testInteractionsRatioFeatureProcessFunction() throws Exception {
		new InteractionsRatioFeatureProcessFunction()
				.applyTo(commentFeatureStream, likeFeatureStream)
				.addSink(new TestSink<>());

		this.env.execute();

		Map<Long, List<Feature>> expectedResults = buildExpectedResults();
		Map<Long, List<Feature>> timestampedResults = TestSink.getResultsTimestamped(Feature.class);

		for (Map.Entry<Long, List<Feature>> expectedResult : expectedResults.entrySet()) {
			List<Feature> actualWindow = timestampedResults.get(expectedResult.getKey());
			assertNotNull(actualWindow,
					"Expected window " + expectedResult.getKey() + " not present in actual results!");

			for (Feature feature : expectedResult.getValue()) {
				assertTrue(actualWindow.remove(feature),
						"Expected result " + feature + " not present in window " + expectedResult.getKey()
								+ actualWindow);
			}
		}

		timestampedResults.entrySet().stream()
				.sorted(Comparator.comparingLong(Map.Entry::getKey))
				.forEach(entry -> assertTrue(entry.getValue().isEmpty(),
						String.format("More results (%s) than expected in window %d", entry.getValue().size(),
								entry.getKey())));

	}

	private Map<Long, List<Feature>> buildExpectedResults() {
		Map<Long, List<Feature>> expectedFeatures = new HashMap<>();

		Stream<Tuple4<Long, Long, Feature.EventType, Feature>> comments = commentTestDataGenerator.getTestData()
				.stream()
				.map(pair -> Tuple4.of(pair.getTimestamp().getMillis(), pair.getElement().getReplyToPostId(),
						Feature.EventType.COMMENT,
						Feature.of(pair.getElement())));

		Stream<Tuple4<Long, Long, Feature.EventType, Feature>> likes = likeTestDataGenerator.getTestData().stream()
				.map(pair -> Tuple4.of(pair.getTimestamp().getMillis(), pair.getElement().getPostId(),
						Feature.EventType.LIKE,
						Feature.of(pair.getElement())));

		Stream<Tuple4<Long, Long, Feature.EventType, Feature>> events = Stream.concat(comments, likes)
				.sorted(Comparator.comparingLong(tuple -> tuple.getField(0)));

		Map<Long, Long> commentCounts = new HashMap<>();
		Map<Long, Long> likeCounts = new HashMap<>();

		events.forEach(event -> {
			Long timestamp = event.getField(0);
			Long postId = event.getField(1);
			Feature.EventType eventType = event.getField(2);
			Feature feature = event.getField(3);
			Long commentCount = commentCounts.getOrDefault(postId, 0L);
			Long likeCount = likeCounts.getOrDefault(postId, 0L);

			if (eventType.equals(Feature.EventType.COMMENT)) {
				commentCounts.put(postId, commentCount + 1);
			} else if (eventType.equals(Feature.EventType.LIKE)) {
				likeCount += 1;
				likeCounts.put(postId, likeCount);

				List<Feature> featureList = expectedFeatures.getOrDefault(timestamp, new ArrayList<>());

				if (commentCount > 0) {
					featureList.add(feature.withFeatureId(Feature.FeatureId.INTERACTIONS_RATIO)
							.withFeatureValue(likeCount.doubleValue() / commentCount.doubleValue()));
				} else {
					featureList.add(feature.withFeatureId(Feature.FeatureId.INTERACTIONS_RATIO)
							.withFeatureValue(1.0));
				}

				expectedFeatures.put(timestamp, featureList);
			}
		});

		return expectedFeatures;
	}

}
