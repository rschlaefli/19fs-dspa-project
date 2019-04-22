package ch.ethz.infk.dspa.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.CommentPostMapping;
import ch.ethz.infk.dspa.stream.connectors.KafkaConsumerBuilder;
import ch.ethz.infk.dspa.stream.connectors.KafkaProducerBuilder;
import ch.ethz.infk.dspa.stream.ops.CommentPostIdEnrichmentProcessFunction;

public class CommentDataStreamBuilder extends AbstractDataStreamBuilder<Comment> {

	private boolean withPostId = false;

	private DataStream<CommentPostMapping> commentPostMappingStream;
	private SinkFunction<CommentPostMapping> commentPostMappingSink;

	public CommentDataStreamBuilder(StreamExecutionEnvironment env) {
		super(env);
	}

	@Override
	public CommentDataStreamBuilder withInputStream(DataStream<Comment> inputStream) {
		super.withInputStream(inputStream);
		return this;
	}

	public CommentDataStreamBuilder withCommentPostMappingStream(DataStream<CommentPostMapping> mappingStream) {
		this.commentPostMappingStream = mappingStream;
		return this;
	}

	public CommentDataStreamBuilder withCommentPostMappingSink(SinkFunction<CommentPostMapping> sink) {
		this.commentPostMappingSink = sink;
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

			if (commentPostMappingStream == null) {
				// if not given, use default kafka source
				ensureValidKafkaConfiguration();

				SourceFunction<CommentPostMapping> source = new KafkaConsumerBuilder<CommentPostMapping>()
						.withTopic("comment-post-mapping")
						.withClass(CommentPostMapping.class)
						.withKafkaConnection(getBootstrapServers(), getGroupId())
						.build();

				commentPostMappingStream = env.addSource(source);
			}

			if (commentPostMappingSink == null) {
				// if not given, use default kafka source
				ensureValidKafkaConfiguration();

				// if not given, use default kafka sink
				commentPostMappingSink = new KafkaProducerBuilder<CommentPostMapping>()
						.withTopic("comment-post-mapping")
						.withClass(CommentPostMapping.class)
						.withKafkaConnection(getBootstrapServers())
						.withExecutionConfig(env.getConfig())
						.build();
			}

			// TODO [nku] maybe add watermarks and timestamps?

			SingleOutputStreamOperator<Comment> enrichedCommentStream = this.stream.connect(commentPostMappingStream)
					.keyBy(new CommentRoutingKeySelector(), CommentPostMapping::getCommentId, TypeInformation.of(Long.class))
					.process(new CommentPostIdEnrichmentProcessFunction())
					.returns(Comment.class);

			// write side output of mappings to sink
			enrichedCommentStream.getSideOutput(CommentPostIdEnrichmentProcessFunction.MAPPING_TAG)
					.addSink(commentPostMappingSink);

			this.stream = enrichedCommentStream;
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
