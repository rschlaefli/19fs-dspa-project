package ch.ethz.infk.dspa.anomalies;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration2.Configuration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import ch.ethz.infk.dspa.AbstractAnalyticsTaskIT;
import ch.ethz.infk.dspa.ResultWindow;
import ch.ethz.infk.dspa.anomalies.dto.EventStatistics;
import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;
import ch.ethz.infk.dspa.anomalies.dto.FraudulentUser;
import ch.ethz.infk.dspa.anomalies.ops.EnsembleProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.ContentsFeatureMapFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.NewUserInteractionFeatureProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.TagCountFeatureMapFunction;
import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.stream.helper.TestSink;

public class AnomaliesAnalyticsTaskIT extends AbstractAnalyticsTaskIT<FraudulentUser> {

	// <personId, prevTs>
	private static Map<Long, Long> previousEventTimestamps = new HashMap<>();

	// <postId, (likeCount, commentCount)>
	private static Map<Long, Tuple2<Long, Long>> previousInteractionCounts = new HashMap<>();

	// <postId, (newUserLikes, totalLikes)>
	private static Map<Long, Tuple2<Long, Long>> previousNewUserLikeCounts = new HashMap<>();

	// <featureId, (count, mean, m2)>
	private static Map<Feature.FeatureId, Tuple3<Long, Double, Double>> rollingStatisticsPerFeature = new HashMap<>();

	// <eventGuid, eventStatistics>
	private static Map<String, EventStatistics> aggregatedEventStatistics = new HashMap<>();

	@Override
	public void beforeEachTaskSpecific(List<Post> allPosts, List<Comment> allComments, List<Like> allLikes)
			throws Exception {
	}

	@Override
	public List<ResultWindow<FraudulentUser>> produceActualResults(DataStream<Post> posts, DataStream<Comment> comments,
			DataStream<Like> likes) throws Exception {

		AnomaliesAnalyticsTask analyticsTask = (AnomaliesAnalyticsTask) new AnomaliesAnalyticsTask()
				.withPropertiesConfiguration(getConfig())
				.withStreamingEnvironment(getEnv())
				.withStaticFilePath(getStaticFilePath())
				.withMaxDelay(getMaxOutOfOrderness())
				.withInputStreams(posts, comments, likes)
				.initialize()
				.build()
				.withSink(new TestSink<>());

		analyticsTask.start();

		final Time tumblingWindowLength = AnomaliesAnalyticsTask.getTumblingOutputWindow(getConfig());

		List<ResultWindow<FraudulentUser>> windows = TestSink
				.getResultsInTumblingWindow(FraudulentUser.class, tumblingWindowLength)
				.entrySet()
				.stream()
				.map(entry -> {
					ResultWindow<FraudulentUser> resultWindow = new ResultWindow<>(entry.getKey(),
							entry.getKey() + tumblingWindowLength.toMilliseconds());
					resultWindow.setResults(entry.getValue());
					return resultWindow;
				})
				.collect(Collectors.toList());

		return windows;
	}

	@Override
	public List<FraudulentUser> buildExpectedResultsOfWindow(WindowAssigner<Object, TimeWindow> assigner,
			TimeWindow window, List<Post> posts, List<Comment> comments, List<Like> likes) throws Exception {

		System.out.println("--- WINDOW ---");
		System.out.println(String.format("Processing window with start=%s end=%s", window.getStart(), window.getEnd()));
		posts.forEach(System.out::println);
		comments.forEach(System.out::println);
		likes.forEach(System.out::println);

		// compose feature streams from all events
		List<Feature> featureStream = composeFeatureStreams(posts, comments, likes);
		System.out.println("--- FEATURES ---");
		featureStream.forEach(System.out::println);

		// apply online averaging
		List<FeatureStatistics> featureStatisticsStream = applyOnlineAveraging(featureStream);
		System.out.println("--- FEATURE STATISTICS ---");
		featureStatisticsStream.forEach(System.out::println);

		// apply ensemble aggregation
		List<EventStatistics> eventStatisticsStream = applyEnsembleAggregation(featureStatisticsStream);
		System.out.println("--- EVENT STATISTICS ---");
		eventStatisticsStream.forEach(System.out::println);

		// compute fraudulent users
		List<FraudulentUser> fraudulentUsers = computeFraudulentUsers(eventStatisticsStream);
		System.out.println("--- FRAUDULENT USERS ---");
		fraudulentUsers.forEach(System.out::println);

		return fraudulentUsers.size() > 0 ? fraudulentUsers : null;
	}

	private List<Feature> composeFeatureStreams(List<Post> posts, List<Comment> comments,
			List<Like> likes)
			throws Exception {
		// compute a timestamp-ordered list of features of all types
		List<Feature> featuresInOrder = Streams.concat(
				posts.stream().map(Feature::of),
				comments.stream().map(Feature::of),
				likes.stream().map(Feature::of))
				.sorted(Comparator.comparingLong(feature -> feature.getCreationDate().getMillis()))
				.collect(Collectors.toList());

		// compute timespan features
		List<Feature> timespanFeatures = computeTimespanFeatures(featuresInOrder);

		// compute contents features for posts and comments
		List<Feature> contentsFeatures = computeContentsFeatures(featuresInOrder);

		// compute tag count features for posts
		List<Feature> tagCountFeatures = computeTagCountFeatures(featuresInOrder);

		// compute interactions ratio features for likes based on like/comment ratio
		List<Feature> interactionsRatioFeatures = computeInteractionsRatioFeatures(featuresInOrder);

		// compute new user interaction features
		final Map<Long, Long> userCreationMap = NewUserInteractionFeatureProcessFunction
				.buildUserCreationRelation(getStaticFilePath() + "person.csv");
		List<Feature> newUserInteractionFeatures = computeNewUserInteractionFeatures(featuresInOrder,
				userCreationMap);

		return Stream
				.of(timespanFeatures, contentsFeatures, tagCountFeatures, interactionsRatioFeatures,
						newUserInteractionFeatures)
				.flatMap(Collection::stream)
				.collect(Collectors.toList());
	}

	private List<FeatureStatistics> applyOnlineAveraging(List<Feature> composedFeatureStream) {
		return composedFeatureStream.stream()
				.map(feature -> {
					if (feature.getFeatureValue() == null) {
						FeatureStatistics featureStatistics = new FeatureStatistics(Feature.copy(feature));
						featureStatistics.getFeature().withFeatureValue(0.0);
						featureStatistics.setMean(0.0);
						featureStatistics.setStdDev(1.0);
						return featureStatistics;
					}

					// get the existing statistics tuple with <count, mean, m2>
					Tuple3<Long, Double, Double> existingStatistics = rollingStatisticsPerFeature
							.getOrDefault(feature.getFeatureId(), Tuple3.of(0L, 0.0, 0.0));

					// increment the count
					existingStatistics.setField(existingStatistics.f0 + 1, 0);

					// update the mean incrementally
					double delta1 = feature.getFeatureValue() - existingStatistics.f1;
					existingStatistics.setField(existingStatistics.f1 + (delta1 / existingStatistics.f0), 1);

					// update the m2 incrementally
					double delta2 = feature.getFeatureValue() - existingStatistics.f1;
					existingStatistics.setField(existingStatistics.f2 + (delta1 * delta2), 2);

					// persist the updated statistics
					rollingStatisticsPerFeature.put(feature.getFeatureId(), existingStatistics);

					// create a new feature statistics
					FeatureStatistics featureStatistics = new FeatureStatistics(feature);
					featureStatistics.setMean(existingStatistics.f1);

					double variance = existingStatistics.f2 / existingStatistics.f0;
					double stdDev = Math.sqrt(variance);
					featureStatistics.setStdDev(!Double.isNaN(stdDev) ? stdDev : 0.0);

					return featureStatistics;
				})
				.collect(Collectors.toList());
	}

	private List<EventStatistics> applyEnsembleAggregation(List<FeatureStatistics> aggregatedFeatureStatistics) {
		ImmutableMap<Feature.FeatureId, Double> thresholds = AnomaliesAnalyticsTask
				.constructEnsembleThresholds(getConfig());

		return aggregatedFeatureStatistics.stream()
				.flatMap(featureStatistics -> {
					EventStatistics eventStatistics = aggregatedEventStatistics
							.getOrDefault(featureStatistics.getEventGUID(), new EventStatistics(thresholds));
					eventStatistics.addFeatureVote(featureStatistics);
					aggregatedEventStatistics.put(featureStatistics.getEventGUID(), eventStatistics);

					Feature.EventType eventType = Feature.EventType
							.valueOf(eventStatistics.getEventGUID().split("_")[0]);
					Set<Feature.FeatureId> votedFeatureIds = eventStatistics.getVotedFeatureIds();
					if (EnsembleProcessFunction.hasAllFeatures(votedFeatureIds, eventType)) {
						return Stream.of(eventStatistics);
					}

					return Stream.of();
				})
				.collect(Collectors.toList());
	}

	private List<FraudulentUser> computeFraudulentUsers(List<EventStatistics> ensembleAggregatedEventStatistics) {
		Configuration config = getConfig();
		double featureEnsembleThreshold = config
				.getDouble("tasks.anomalies.fraudulentUsers.featureEnsembleThreshold");
		double fraudulentEventsThreshold = config
				.getDouble("tasks.anomalies.fraudulentUsers.fraudulentEventsThreshold");

		// <personId, (anomalousEvents, totalEvents)>
		Map<Long, Tuple2<List<EventStatistics>, Long>> personAnomalousEventCounts = new HashMap<>();
		ensembleAggregatedEventStatistics
				.forEach(eventStatistics -> {
					personAnomalousEventCounts.compute(eventStatistics.getPersonId(),
							(personId, anomalousAndTotalEvents) -> {
								if (anomalousAndTotalEvents == null) {
									anomalousAndTotalEvents = Tuple2.of(new ArrayList<>(), 1L);
								}

								if (eventStatistics.getIsAnomalousWithMajority(featureEnsembleThreshold)) {
									anomalousAndTotalEvents.f0.add(eventStatistics);
								}

								anomalousAndTotalEvents.setField(anomalousAndTotalEvents.f1 + 1, 1);

								return anomalousAndTotalEvents;
							});
				});

		List<FraudulentUser> resultList = personAnomalousEventCounts.entrySet().stream()
				.filter(entry -> (((double) entry.getValue().f0.size())
						/ entry.getValue().f1.doubleValue()) > fraudulentEventsThreshold)
				.map(entry -> {
					FraudulentUser fraudulentUser = new FraudulentUser(entry.getKey());
					entry.getValue().f0.forEach(fraudulentUser::withVotesFrom);
					return fraudulentUser;
				})
				.collect(Collectors.toList());

		return resultList;
	}

	private List<Feature> computeTimespanFeatures(List<Feature> featuresInOrder) {
		return featuresInOrder.stream()
				.map(Feature::copy)
				.map(feature -> {

					long featureTimestamp = feature.getCreationDate().getMillis();
					double previousEventTimestamp = previousEventTimestamps
							.getOrDefault(feature.getPersonId(), featureTimestamp)
							.doubleValue();

					previousEventTimestamps.put(feature.getPersonId(), featureTimestamp);

					return feature
							.withFeatureId(Feature.FeatureId.TIMESPAN)
							.withFeatureValue(featureTimestamp - previousEventTimestamp);
				})
				.collect(Collectors.toList());
	}

	private List<Feature> computeInteractionsRatioFeatures(List<Feature> featuresInOrder) {

		List<Feature> interactionsRatioFeatures = featuresInOrder.stream()
				.filter(feature -> feature.getEventType() != Feature.EventType.POST)
				.map(Feature::copy)
				.flatMap(feature -> {
					Tuple2<Long, Long> previousCounts = previousInteractionCounts.getOrDefault(feature.getPersonId(),
							Tuple2.of(0L, 0L));

					if (feature.getEventType().equals(Feature.EventType.LIKE)) {
						previousCounts.setField(previousCounts.f0 + 1, 0);
					} else {
						previousCounts.setField(previousCounts.f1 + 1, 1);
					}

					previousInteractionCounts.put(feature.getPersonId(), previousCounts);

					// likes should produce a new feature
					if (feature.getEventType().equals(Feature.EventType.LIKE)) {
						Feature intermediateResult = feature.withFeatureId(Feature.FeatureId.INTERACTIONS_RATIO);

						if (previousCounts.f1 == 0) {
							// if there are no comments but at least one like, 100% are likes
							if (previousCounts.f0 > 0) {
								return Stream.of(intermediateResult.withFeatureValue(1.0));
							}

							// if there are no comments and no likes, the ratio is zero
							// this will not be reached, as the counts have been incremented before
							return Stream.of(intermediateResult.withFeatureValue(0.0));
						}

						return Stream.of(intermediateResult
								.withFeatureValue(previousCounts.f0.doubleValue() / previousCounts.f1));
					}

					// return an empty stream for comment features to remove them from the result
					return Stream.of();
				})
				.collect(Collectors.toList());

		return interactionsRatioFeatures;
	}

	private List<Feature> computeNewUserInteractionFeatures(List<Feature> featuresInOrder,
			Map<Long, Long> userCreationMap) {
		final long newUserThreshold = Time
				.hours(getConfig().getInt("tasks.anomalies.features.newUserLikes.newUserThresholdInHours"))
				.toMilliseconds();

		List<Feature> newUserInteractionFeatures = featuresInOrder.stream()
				.filter(feature -> feature.getEventType() == Feature.EventType.LIKE)
				.map(Feature::copy)
				.map(feature -> {
					Tuple2<Long, Long> previousNewUserLikeCount = previousNewUserLikeCounts
							.getOrDefault(feature.getPostId(), Tuple2.of(0L, 0L));

					// increment the total like count
					previousNewUserLikeCount.setField(previousNewUserLikeCount.f1 + 1, 1);

					// check whether the author of the like is a new user
					long userCreationTimestamp = userCreationMap.getOrDefault(feature.getPostId(), 0L);
					if (feature.getCreationDate().getMillis() - userCreationTimestamp < newUserThreshold) {
						// increment the new user like count
						previousNewUserLikeCount.setField(previousNewUserLikeCount.f0 + 1, 0);
					}

					previousNewUserLikeCounts.put(feature.getPostId(), previousNewUserLikeCount);

					return feature
							.withFeatureId(Feature.FeatureId.NEW_USER_LIKES)
							.withFeatureValue(previousNewUserLikeCount.f0.doubleValue() / previousNewUserLikeCount.f1);
				})
				.collect(Collectors.toList());

		return newUserInteractionFeatures;
	}

	private List<Feature> computeContentsFeatures(List<Feature> featuresInOrder) {
		Configuration config = getConfig();

		ContentsFeatureMapFunction contentsMapper = new ContentsFeatureMapFunction(
				config.getInt("tasks.anomalies.features.contents.short.maxLength"),
				config.getInt("tasks.anomalies.features.contents.long.minLength"));

		return featuresInOrder.stream()
				.filter(feature -> feature.getEventType() != Feature.EventType.LIKE)
				.map(Feature::copy)
				.map(contentsMapper::map)
				.collect(Collectors.toList());
	}

	private List<Feature> computeTagCountFeatures(List<Feature> featuresInOrder) {
		TagCountFeatureMapFunction tagCountMapper = new TagCountFeatureMapFunction();
		return featuresInOrder.stream()
				.filter(feature -> feature.getEventType() == Feature.EventType.POST)
				.map(Feature::copy)
				.map(tagCountMapper::map)
				.collect(Collectors.toList());
	}

	@Override
	public List<WindowAssigner<Object, TimeWindow>> getWindowAssigners() {
		return Collections.singletonList(TumblingEventTimeWindows
				.of(Time.hours(getConfig().getLong("tasks.anomalies.fraudulentUsers.updateIntervalInHours"))));
	}

	@Override
	public Time getMaxOutOfOrderness() {
		return Time.seconds(600);
	}
}
