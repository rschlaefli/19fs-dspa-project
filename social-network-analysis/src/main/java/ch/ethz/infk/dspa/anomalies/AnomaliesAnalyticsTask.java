package ch.ethz.infk.dspa.anomalies;

import org.apache.commons.configuration2.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.google.common.collect.ImmutableMap;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import ch.ethz.infk.dspa.anomalies.dto.EventStatistics;
import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;
import ch.ethz.infk.dspa.anomalies.dto.FraudulentUser;
import ch.ethz.infk.dspa.anomalies.ops.EnsembleProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.EventStatisticsWindowProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.OnlineAverageProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.ContentsFeatureMapFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.InteractionsRatioFeatureProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.NewUserInteractionFeatureProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.TagCountFeatureMapFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.TimespanFeatureProcessFunction;

public class AnomaliesAnalyticsTask
		extends AbstractAnalyticsTask<SingleOutputStreamOperator<FraudulentUser>, FraudulentUser> {

	@Override
	public AnomaliesAnalyticsTask initialize() throws Exception {
		this.withKafkaConsumerGroup("anomalies");
		super.initialize();
		return this;
	}

	@Override
	public AnomaliesAnalyticsTask build() {
		DataStream<Feature> featureStream = composeFeatureStream();

		SingleOutputStreamOperator<FeatureStatistics> featureStatisticsStream = applyOnlineAveraging(featureStream);

		SingleOutputStreamOperator<EventStatistics> eventStatisticsStream = applyEnsembleAggregation(
				featureStatisticsStream);

		SingleOutputStreamOperator<FraudulentUser> fraudulentUserStream = computeFraudulentUsers(eventStatisticsStream);

		this.outputStream = fraudulentUserStream;

		return this;
	}

	public DataStream<Feature> composeFeatureStream() {
		final int contentsShortUntilLength = this.config.getInt("tasks.anomalies.features.contents.short.maxLength");
		final int contentsLongFromLength = this.config.getInt("tasks.anomalies.features.contents.long.minLength");
		final Time newUserThreshold = Time.hours(this.config
				.getInt("tasks.anomalies.features.newUserLikes.newUserThresholdInHours"));
		final String staticPerson = this.getStaticFilePath() + "person.csv";

		// map the input streams to separate features
		DataStream<Feature> postFeatureStream = this.postStream
				.map(Feature::of)
				.returns(Feature.class);
		DataStream<Feature> commentFeatureStream = this.commentStream
				.map(Feature::of)
				.returns(Feature.class);
		DataStream<Feature> likeFeatureStream = this.likeStream
				.map(Feature::of)
				.returns(Feature.class);

		// compute features over the stream
		DataStream<Feature> timespanFeatureStream = new TimespanFeatureProcessFunction().applyTo(postFeatureStream,
				commentFeatureStream, likeFeatureStream);

		DataStream<Feature> contentsFeatureStream = new ContentsFeatureMapFunction(contentsShortUntilLength,
				contentsLongFromLength).applyTo(postFeatureStream, commentFeatureStream);

		DataStream<Feature> tagCountFeatureStream = new TagCountFeatureMapFunction().applyTo(postFeatureStream);

		DataStream<Feature> newUserInteractionFeatureStream = new NewUserInteractionFeatureProcessFunction(
				newUserThreshold, staticPerson).applyTo(likeFeatureStream);

		DataStream<Feature> interactionsRatioFeatureStream = new InteractionsRatioFeatureProcessFunction()
				.applyTo(commentFeatureStream, likeFeatureStream);

		// merge feature streams into a single one
		return timespanFeatureStream.union(contentsFeatureStream, tagCountFeatureStream,
				newUserInteractionFeatureStream, interactionsRatioFeatureStream);
	}

	SingleOutputStreamOperator<FeatureStatistics> applyOnlineAveraging(DataStream<Feature> featureStream) {
		// combine all feature streams into a feature statistics stream
		// computed using the rolling mean operator over all features with the same feature id
		// mapped to contain the anomaly decision of each separate feature
		return featureStream
				.keyBy(feature -> feature.getFeatureId().getId())
				.process(new OnlineAverageProcessFunction());
	}

	SingleOutputStreamOperator<EventStatistics> applyEnsembleAggregation(
			SingleOutputStreamOperator<FeatureStatistics> featureStatisticsStream) {
		ImmutableMap<Feature.FeatureId, Double> thresholds = constructEnsembleThresholds(this.config);

		// analyze events based on all their computed feature statistics
		// apply an ensemble decision over all of these statistics
		return featureStatisticsStream
				.keyBy(FeatureStatistics::getEventGUID)
				.process(new EnsembleProcessFunction(thresholds));
	}

	SingleOutputStreamOperator<FraudulentUser> computeFraudulentUsers(
			SingleOutputStreamOperator<EventStatistics> eventStatisticsStream) {
		Time fraudulentUserUpdateInterval = Time
				.hours(this.config.getLong("tasks.anomalies.fraudulentUsers.updateIntervalInHours"));
		double featureEnsembleThreshold = this.config
				.getDouble("tasks.anomalies.fraudulentUsers.featureEnsembleThreshold");
		double fraudulentEventsThreshold = this.config
				.getDouble("tasks.anomalies.fraudulentUsers.fraudulentEventsThreshold");

		// extract all events that are deemed anomalous based on the majority decision
		// for each user, check whether there are more than allowed anomalous events within a given
		// timeframe
		// finally, output all fraudulent users alongside an overview of all anomalous feature decisions
		return eventStatisticsStream
				.keyBy(EventStatistics::getPersonId)
				.window(TumblingEventTimeWindows.of(fraudulentUserUpdateInterval))
				.process(new EventStatisticsWindowProcessFunction(featureEnsembleThreshold,
						fraudulentEventsThreshold));
	}

	@Override
	public void start() throws Exception {
		super.start("Unusual Activities");
	}

	@Override
	protected Time getTumblingOutputWindow() {
		return getTumblingOutputWindow(this.config);
	}

	public static Time getTumblingOutputWindow(Configuration config) {
		return Time.hours(config.getLong("tasks.anomalies.fraudulentUsers.updateIntervalInHours"));
	}

	public static ImmutableMap<Feature.FeatureId, Double> constructEnsembleThresholds(Configuration config) {
		// construct a map of thresholds for maximum expected deviations from mean
		final ImmutableMap<Feature.FeatureId, Double> thresholds = new ImmutableMap.Builder<Feature.FeatureId, Double>()
				.put(Feature.FeatureId.TIMESPAN, config.getDouble("tasks.anomalies.features.timespan.threshold"))
				.put(Feature.FeatureId.CONTENTS_SHORT,
						config.getDouble("tasks.anomalies.features.contents.short.threshold"))
				.put(Feature.FeatureId.CONTENTS_MEDIUM,
						config.getDouble("tasks.anomalies.features.contents.medium.threshold"))
				.put(Feature.FeatureId.CONTENTS_LONG,
						config.getDouble("tasks.anomalies.features.contents.long.threshold"))
				.put(Feature.FeatureId.TAG_COUNT,
						config.getDouble("tasks.anomalies.features.tagCount.threshold"))
				.put(Feature.FeatureId.NEW_USER_LIKES,
						config.getDouble("tasks.anomalies.features.newUserLikes.threshold"))
				.put(Feature.FeatureId.INTERACTIONS_RATIO,
						config.getDouble("tasks.anomalies.features.interactionsRatio.threshold"))
				.put(Feature.FeatureId.CONTENTS_EMPTY, 0.0)
				.build();

		return thresholds;
	}
}
