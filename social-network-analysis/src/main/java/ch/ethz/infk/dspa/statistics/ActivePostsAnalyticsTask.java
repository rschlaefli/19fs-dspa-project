package ch.ethz.infk.dspa.statistics;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;
import ch.ethz.infk.dspa.statistics.dto.StatisticsOutput;
import ch.ethz.infk.dspa.statistics.ops.TypeCountAggregateFunction;
import ch.ethz.infk.dspa.statistics.ops.UniquePersonProcessFunction;

public class ActivePostsAnalyticsTask extends AbstractAnalyticsTask<DataStream<StatisticsOutput>, StatisticsOutput> {

	@Override
	public ActivePostsAnalyticsTask initialize() throws Exception {
		this.withKafkaConsumerGroup("active-posts");
		super.initialize();
		return this;
	}

	@Override
	public ActivePostsAnalyticsTask build() {
		final Time windowLength = Time.hours(this.config.getLong("tasks.statistics.window.lengthInHours"));
		final Time windowSlide = Time.minutes(this.config.getLong("tasks.statistics.window.slideInMinutes"));
		final Time uniquePeopleUpdateInterval = Time
				.hours(this.config.getLong("tasks.statistics.uniquePeople.updateIntervalInHours"));

		// map data streams to activity streams
		DataStream<PostActivity> postActivityStream = this.postStream
				.map(PostActivity::of)
				.returns(PostActivity.class);
		DataStream<PostActivity> likeActivityStream = this.likeStream
				.map(PostActivity::of)
				.returns(PostActivity.class);
		DataStream<PostActivity> commentActivityStream = this.commentStream
				.map(PostActivity::of)
				.returns(PostActivity.class);

		// merge activity streams
		KeyedStream<PostActivity, Long> activityStream = postActivityStream
				.union(likeActivityStream, commentActivityStream) // merge streams together
				.keyBy(PostActivity::getPostId); // key by post id;

		// create windows for subsequent aggregations to work on
		WindowedStream<PostActivity, Long, TimeWindow> windowedActivityStream = activityStream
				.window(SlidingEventTimeWindows.of(windowLength, windowSlide));

		// comment count
		SingleOutputStreamOperator<StatisticsOutput> commentCountStream = windowedActivityStream
				.aggregate(new TypeCountAggregateFunction(ActivityType.COMMENT));

		// reply count
		SingleOutputStreamOperator<StatisticsOutput> replyCountStream = windowedActivityStream
				.aggregate(new TypeCountAggregateFunction(ActivityType.REPLY));

		// unique people count
		SingleOutputStreamOperator<StatisticsOutput> uniquePersonCountStream = activityStream
				.process(new UniquePersonProcessFunction(uniquePeopleUpdateInterval, windowLength));

		this.outputStream = commentCountStream
				.union(replyCountStream, uniquePersonCountStream);

		return this;
	}

	@Override
	public void start() throws Exception {
		super.start("Active Post Statistics");
	}

	@Override
	protected Time getTumblingOutputWindow() {
		return Time.minutes(this.config.getLong("tasks.statistics.window.slideInMinutes"));
	}
}
