package ch.ethz.infk.dspa.anomalies;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import ch.ethz.infk.dspa.anomalies.dto.EventStatistics;
import ch.ethz.infk.dspa.anomalies.dto.Feature;

import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;
import ch.ethz.infk.dspa.anomalies.dto.FraudulentUser;
import ch.ethz.infk.dspa.anomalies.ops.EnsembleAggregationFunction;
import ch.ethz.infk.dspa.anomalies.ops.EventStatisticsWindowProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.OnlineAverageProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.ContentsFeatureProcessFunction;
import ch.ethz.infk.dspa.anomalies.ops.features.TimespanFeatureProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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
		// stage 1
		DataStream<Feature> featureStream = composeFeatureStream();

		// stage 2
		SingleOutputStreamOperator<FeatureStatistics> featureStatisticsStream = applyOnlineAveraging(featureStream);

		// stage 3
		SingleOutputStreamOperator<EventStatistics> eventStatisticsStream = applyEnsembleAggregation(featureStatisticsStream);

		// stage 4
		SingleOutputStreamOperator<FraudulentUser> fraudulentUserStream = computeFraudulentUsers(eventStatisticsStream);

		this.outputStream = fraudulentUserStream;

		return this;
	}

	DataStream<Feature> composeFeatureStream() {
		// map the input streams to separate features
		DataStream<Feature> postFeatureStream = this.postStream.map(Feature::of).returns(Feature.class);
		DataStream<Feature> commentFeatureStream = this.commentStream.map(Feature::of).returns(Feature.class);
		DataStream<Feature> likeFeatureStream = this.likeStream.map(Feature::of).returns(Feature.class);

		// compute features over the stream
		DataStream<Feature> timespanFeatureStream = TimespanFeatureProcessFunction.applyTo(postFeatureStream, commentFeatureStream, likeFeatureStream);
		DataStream<Feature> contentsFeatureStream = ContentsFeatureProcessFunction.applyTo(postFeatureStream, commentFeatureStream);

		// merge feature streams into a single one
		return timespanFeatureStream.union(contentsFeatureStream);
	}

	SingleOutputStreamOperator<FeatureStatistics> applyOnlineAveraging(DataStream<Feature> featureStream) {
		// combine all feature streams into a feature statistics stream
		// computed using the rolling mean operator over all features with the same feature id
		// mapped to contain the anomaly decision of each separate feature
		return featureStream
				.keyBy(Feature::getFeatureId)
				.process(new OnlineAverageProcessFunction());
	}

	SingleOutputStreamOperator<EventStatistics> applyEnsembleAggregation(SingleOutputStreamOperator<FeatureStatistics> featureStatisticsStream) {
		// analyze events based on all their computed feature statistics
		// apply an ensemble decision over all of these statistics
		return featureStatisticsStream
				.keyBy(FeatureStatistics::getEventGUID)
				.window(EventTimeSessionWindows.withGap(Time.minutes(15)))
				.aggregate(new EnsembleAggregationFunction());
	}

	SingleOutputStreamOperator<FraudulentUser> computeFraudulentUsers(SingleOutputStreamOperator<EventStatistics> eventStatisticsStream) {
		// extract all events that are deemed anomalous based on the majority decision
		// for each user, check whether there are more than allowed anomalous events within a given timeframe
		// finally, output all fraudulent users alongside an overview of all anomalous feature decisions
		return eventStatisticsStream
				.keyBy(EventStatistics::getPersonId)
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.process(new EventStatisticsWindowProcessFunction());
	}
}
