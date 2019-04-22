package ch.ethz.infk.dspa.statistics;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;
import ch.ethz.infk.dspa.statistics.ops.TypeCountAggregateFunction;
import ch.ethz.infk.dspa.stream.CommentDataStreamBuilder;
import ch.ethz.infk.dspa.stream.LikeDataStreamBuilder;
import ch.ethz.infk.dspa.stream.PostDataStreamBuilder;
import org.apache.flink.util.Collector;

public class ActivePostsAnalyticsTask extends AbstractAnalyticsTask<KeyedStream<PostActivity, Long>, PostActivity> {

	@Override
	public ActivePostsAnalyticsTask initialize() throws Exception {
		this.withKafkaConsumerGroup("active-posts");
		super.initialize();
		return this;
	}

	@Override
	public ActivePostsAnalyticsTask build() {
		// map data streams to activity streams
		DataStream<PostActivity> postActivityStream = this.postStream.map(PostActivity::of);
		DataStream<PostActivity> likeActivityStream = this.likeStream.map(PostActivity::of);
		DataStream<PostActivity> commentActivityStream = this.commentStream.map(PostActivity::of);

		// merge activity streams
		KeyedStream<PostActivity, Long> activityStream = postActivityStream
				.union(likeActivityStream, commentActivityStream) // merge streams together
				.keyBy(PostActivity::getPostId); // key by post id;

		// comment count
		activityStream.window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)))
				.aggregate(new TypeCountAggregateFunction(ActivityType.COMMENT))
				.map(count -> "Comments: " + count)
				.print();

		// reply count
		activityStream.window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)))
				.aggregate(new TypeCountAggregateFunction(ActivityType.REPLY))
				.map(count -> "Replies: " + count)
				.print();

		// activityStream.print();

		// unique people count
		// tupleStream.window(SlidingEventTimeWindows.of(Time.hours(12),
		// Time.hours(1)));

		this.outputStream = activityStream;

		return this;
	}
}
