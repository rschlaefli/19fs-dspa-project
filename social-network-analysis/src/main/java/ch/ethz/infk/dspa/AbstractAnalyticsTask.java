package ch.ethz.infk.dspa;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.CommentPostMapping;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.stream.CommentDataStreamBuilder;
import ch.ethz.infk.dspa.stream.LikeDataStreamBuilder;
import ch.ethz.infk.dspa.stream.PostDataStreamBuilder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public abstract class AbstractAnalyticsTask<OUT_STREAM extends DataStream<OUT_TYPE>, OUT_TYPE> {

    private Time maxDelay;

    private String bootstrapServers;
    private String groupId;

    private StreamExecutionEnvironment env;
    public DataStream<Post> postStream;
    public DataStream<Comment> commentStream;
    public DataStream<CommentPostMapping> commentMappingStream;
    public SinkFunction<CommentPostMapping> commentMappingSink;
    public DataStream<Like> likeStream;
    public OUT_STREAM outputStream;

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withStreamingEnvironment(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withKafkaServer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withKafkaConsumerGroup(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withMaxDelay(Time maxDelay) {
        this.maxDelay = maxDelay;
        return this;
    }

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withPostStream(DataStream<Post> postStream) {
        this.postStream = postStream;
        return this;
    }

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withCommentStream(DataStream<Comment> commentStream) {
        this.commentStream = commentStream;
        return this;
    }

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withLikeStream(DataStream<Like> likeStream) {
        this.likeStream = likeStream;
        return this;
    }

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withInputStreams(DataStream<Post> postStream, DataStream<Comment> commentStream, DataStream<Like> likeStream) {
        return this
                .withPostStream(postStream)
                .withCommentStream(commentStream)
                .withLikeStream(likeStream);
    }

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withCommentPostMappingConfig(
            DataStream<CommentPostMapping> commentMappingStream, SinkFunction<CommentPostMapping> commentMappingSink) {
        this.commentMappingStream = commentMappingStream;
        this.commentMappingSink = commentMappingSink;
        return this;
    }

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> initialize() throws Exception {
        if (this.maxDelay == null) {
            throw new IllegalArgumentException("MISSING_MAX_DELAY");
        }

        if (this.bootstrapServers == null) {
            this.bootstrapServers = "localhost:9092";
        }

        if (this.env == null) {
            this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        if (this.postStream == null) {
            withPostStream(new PostDataStreamBuilder(this.env)
                    .withKafkaConnection(this.bootstrapServers, this.groupId)
                    .withMaxOutOfOrderness(this.maxDelay)
                    .build());
        } else {
            withPostStream(new PostDataStreamBuilder(this.env)
                    .withInputStream(this.postStream)
                    .withMaxOutOfOrderness(this.maxDelay)
                    .build());
        }

        if (this.commentStream == null || this.commentMappingStream == null || this.commentMappingSink == null) {
            withCommentStream(new CommentDataStreamBuilder(this.env)
                    .withPostIdEnriched()
                    .withKafkaConnection(this.bootstrapServers, this.groupId)
                    .withMaxOutOfOrderness(this.maxDelay)
                    .build());
        } else {
            withCommentStream(new CommentDataStreamBuilder(this.env)
                    .withInputStream(this.commentStream)
                    .withCommentPostMappingStream(this.commentMappingStream)
                    .withCommentPostMappingSink(this.commentMappingSink)
                    .withPostIdEnriched()
                    .withMaxOutOfOrderness(this.maxDelay)
                    .build());
        }

        if (this.likeStream == null) {
            withLikeStream(new LikeDataStreamBuilder(this.env)
                    .withKafkaConnection(this.bootstrapServers, this.groupId)
                    .withMaxOutOfOrderness(this.maxDelay)
                    .build());
        } else {
            withLikeStream(new LikeDataStreamBuilder(this.env)
                    .withInputStream(this.likeStream)
                    .withMaxOutOfOrderness(this.maxDelay)
                    .build());
        }

        return this;
    }

    public abstract AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> build();

    public AbstractAnalyticsTask<OUT_STREAM, OUT_TYPE> withSink(SinkFunction<OUT_TYPE> sinkFunction) {
        this.outputStream.addSink(sinkFunction);
        return this;
    }

    public void start() throws Exception {
        this.env.execute("Flink Streaming Social Network Analysis");
    }
}
