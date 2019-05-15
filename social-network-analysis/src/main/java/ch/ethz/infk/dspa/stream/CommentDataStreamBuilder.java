package ch.ethz.infk.dspa.stream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.stream.connectors.KafkaConsumerBuilder;
import ch.ethz.infk.dspa.stream.dto.CommentPostMapping;
import ch.ethz.infk.dspa.stream.ops.CommentPostIdEnrichmentBroadcastProcessFunction;

public class CommentDataStreamBuilder extends AbstractDataStreamBuilder<Comment> {

	private boolean withPostId = false;

	public CommentDataStreamBuilder(StreamExecutionEnvironment env) {
		super(env);
	}

	@Override
	public CommentDataStreamBuilder withInputStream(DataStream<Comment> inputStream) {
		super.withInputStream(inputStream);
		return this;
	}

	public CommentDataStreamBuilder withPostIdEnriched() {
		this.withPostId = true;
		return this;
	}

	@Override
	public DataStream<Comment> build() {
		if (this.stream == null) {
			// if not given, use default kafka source
			ensureValidKafkaConfiguration();

			SourceFunction<Comment> source = new KafkaConsumerBuilder<Comment>()
					.withTopic("comment")
					.withClass(Comment.class)
					.withKafkaConnection(getBootstrapServers(), getGroupId())
					.build();
			this.stream = env.addSource(source);
		}

		this.stream = this.stream.assignTimestampsAndWatermarks(
				new BoundedOutOfOrdernessTimestampExtractor<Comment>(getMaxOutOfOrderness()) {
					private static final long serialVersionUID = 1L;

					@Override
					public long extractTimestamp(Comment element) {
						return element.getCreationDate().getMillis();
					}
				});

		if (withPostId) {

			// map comments to triple of id, parentCommentId, parentPostId and broadcast them
			BroadcastStream<CommentPostMapping> broadcastedMappingStream = this.stream
					.map(comment -> CommentPostMapping.of(comment.getId(), comment.getReplyToCommentId(),
							comment.getReplyToPostId()))
					.broadcast(CommentPostIdEnrichmentBroadcastProcessFunction.COMMENT_POST_MAPPING_DESCRIPTOR,
							CommentPostIdEnrichmentBroadcastProcessFunction.BUFFER_DESCRIPTOR,
							CommentPostIdEnrichmentBroadcastProcessFunction.POST_WINDOW_DESCRIPTOR);

			// connect mappings with comments and add postId to comments
			this.stream = this.stream.keyBy(Comment::getId)
					.connect(broadcastedMappingStream)
					// TODO [nku] extract as param
					.process(new CommentPostIdEnrichmentBroadcastProcessFunction(Time.hours(10000)));

		}

		return this.stream;
	}

	public static class CommentRoutingKeySelector implements KeySelector<Comment, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(Comment comment) throws Exception {
			if (comment.getReplyToPostId() != null) {
				return comment.getId();
			} else {
				return comment.getReplyToCommentId();
			}
		}
	}

}
