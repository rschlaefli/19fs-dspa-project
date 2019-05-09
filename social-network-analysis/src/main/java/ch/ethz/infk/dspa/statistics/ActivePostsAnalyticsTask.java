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
import ch.ethz.infk.dspa.statistics.dto.PostActivityCount;
import ch.ethz.infk.dspa.statistics.ops.TypeCountAggregateFunction;
import ch.ethz.infk.dspa.statistics.ops.UniquePersonProcessFunction;
import ch.ethz.infk.dspa.statistics.ops.WindowInfoProcessFunction;

public class ActivePostsAnalyticsTask extends AbstractAnalyticsTask<DataStream<String>, String> {

	@Override
	public ActivePostsAnalyticsTask initialize() throws Exception {
		this.withKafkaConsumerGroup("active-posts");
		super.initialize();
		return this;
	}

	@Override
	public ActivePostsAnalyticsTask build() {
		// map data streams to activity streams
		DataStream<PostActivity> postActivityStream = this.postStream.map(PostActivity::of).returns(PostActivity.class);
		DataStream<PostActivity> likeActivityStream = this.likeStream.map(PostActivity::of).returns(PostActivity.class);
		DataStream<PostActivity> commentActivityStream = this.commentStream.map(PostActivity::of)
				.returns(PostActivity.class);

		// merge activity streams
		KeyedStream<PostActivity, Long> activityStream = postActivityStream
				.union(likeActivityStream, commentActivityStream) // merge streams together
				.keyBy(PostActivity::getPostId); // key by post id;

		// create windows for subsequent aggregations to work on
		WindowedStream<PostActivity, Long, TimeWindow> windowedActivityStream = activityStream
				.window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)));

		// comment count
		SingleOutputStreamOperator<String> commentCountStream = windowedActivityStream
				.aggregate(new TypeCountAggregateFunction(ActivityType.COMMENT))
				.process(new WindowInfoProcessFunction<PostActivityCount>())
				.map(result -> String.format("%d;%s", result.f0, result.f1.toCsv()));

		// reply count
		SingleOutputStreamOperator<String> replyCountStream = windowedActivityStream
				.aggregate(new TypeCountAggregateFunction(ActivityType.REPLY))
				.process(new WindowInfoProcessFunction<PostActivityCount>())
				.map(result -> String.format("%d;%s", result.f0, result.f1.toCsv()));

		// unique people count
		SingleOutputStreamOperator<String> uniquePersonCountStream = activityStream
				.process(new UniquePersonProcessFunction())
				.map(result -> String.format("%d;%d;PEOPLE;%d", result.f1, result.f0, result.f2));

		this.outputStream = commentCountStream.union(
				replyCountStream,
				uniquePersonCountStream);

		return this;
	}
}
