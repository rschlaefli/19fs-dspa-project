package ch.ethz.infk.dspa.statistics;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;
import ch.ethz.infk.dspa.statistics.ops.TypeCountAggregateFunction;
import ch.ethz.infk.dspa.stream.CommentDataStreamBuilder;
import ch.ethz.infk.dspa.stream.LikeDataStreamBuilder;
import ch.ethz.infk.dspa.stream.PostDataStreamBuilder;

public class ActivePostsStatistics {

	private final static Logger LOG = LogManager.getLogger();

	String bootstrapServers = "localhost:9092";
	String groupId = "active-posts";

	public ActivePostsStatistics withKafkaServer(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
		return this;
	}

	public void start() {

		// build stream execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		long maxDelay = 600;

		// build data streams
		DataStream<Post> postStream = new PostDataStreamBuilder(env)
				.withKafkaConnection(bootstrapServers, groupId)
				.withMaxOutOfOrderness(Time.seconds(maxDelay))
				.build();

		DataStream<Comment> commentStream = new CommentDataStreamBuilder(env)
				.withPostIdEnriched()
				.withKafkaConnection(bootstrapServers, groupId)
				.withMaxOutOfOrderness(Time.seconds(maxDelay))
				.build();

		DataStream<Like> likeStream = new LikeDataStreamBuilder(env)
				.withKafkaConnection(bootstrapServers, groupId)
				.withMaxOutOfOrderness(Time.seconds(maxDelay))
				.build();

		// map data streams to activity streams
		DataStream<PostActivity> postActivityStream = postStream
				.map(post -> new PostActivity(post.getId(), ActivityType.POST, post.getPersonId()));

		DataStream<PostActivity> likeActivityStream = likeStream
				.map(like -> new PostActivity(like.getPostId(), ActivityType.LIKE, like.getPersonId()));

		DataStream<PostActivity> commentActivityStream = commentStream
				.map(comment -> {
					ActivityType type = comment.getReplyToCommentId() == null ? ActivityType.COMMENT
							: ActivityType.REPLY;
					return new PostActivity(comment.getReplyToPostId(), type, comment.getPersonId());
				});

		// merge activity streams
		KeyedStream<PostActivity, Long> activityStream = postActivityStream
				.union(likeActivityStream, commentActivityStream) // merge streams together
				.keyBy(activity -> activity.getPostId()); // key by post id;

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

		activityStream.print();

		// unique people count
		// tupleStream.window(SlidingEventTimeWindows.of(Time.hours(12),
		// Time.hours(1)));

		// execute program
		try {
			env.execute("Flink Streaming Social Network Analysis");
		} catch (Exception e) {
			// TODO [nku] error handling
			e.printStackTrace();
		}
	}

}
