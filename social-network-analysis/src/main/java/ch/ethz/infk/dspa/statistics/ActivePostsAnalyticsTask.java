package ch.ethz.infk.dspa.statistics;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import ch.ethz.infk.dspa.statistics.ops.UniquePersonProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;
import ch.ethz.infk.dspa.statistics.ops.TypeCountAggregateFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

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
		DataStream<PostActivity> commentActivityStream = this.commentStream.map(PostActivity::of).returns(PostActivity.class);

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
				.map(count -> "Comments: " + count);

		// reply count
		SingleOutputStreamOperator<String> replyCountStream = windowedActivityStream
				.aggregate(new TypeCountAggregateFunction(ActivityType.REPLY))
				.map(count -> "Replies: " + count);

		// unique people count
		SingleOutputStreamOperator<String> uniquePersonCountStream = activityStream
				.process(new UniquePersonProcessFunction())
				.map(result -> "PostId=" + result.f0 + " WindowStart=" + result.f1 + " UniquePeople=" + result.f2);

		this.outputStream = commentCountStream.union(
				replyCountStream,
				uniquePersonCountStream
		);

		return this;
	}
}
