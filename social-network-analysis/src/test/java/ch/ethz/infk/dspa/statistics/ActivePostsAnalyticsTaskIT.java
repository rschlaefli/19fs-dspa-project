package ch.ethz.infk.dspa.statistics;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.CommentPostMapping;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.stream.CommentDataStreamBuilder;
import ch.ethz.infk.dspa.stream.helper.SourceSink;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.LikeTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

public class ActivePostsAnalyticsTaskIT extends AbstractTestBase {

    private StreamExecutionEnvironment env;
    private DataStream<Post> postStream;
    private DataStream<Comment> commentStream;
    private DataStream<Like> likeStream;

    private SourceSink mappingSourceSink;
    private DataStream<CommentPostMapping> mappingStream;

    @BeforeEach
    public void setup() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        postStream = new PostTestDataGenerator().generate(env, "./../data/test/01_test/post_event_stream.csv");
        likeStream = new LikeTestDataGenerator().generate(env, "./../data/test/01_test/likes_event_stream.csv");
        commentStream = new CommentTestDataGenerator().generate(env, "./../data/test/01_test/comment_event_stream.csv");

        mappingSourceSink = CommentTestDataGenerator.generateSourceSink("./../data/test/01_test/comment_event_stream.csv");
        mappingStream = env.addSource(mappingSourceSink);
    }

    @Test
    public void testActivePostsAnalyticsTask() throws Exception {
        ActivePostsAnalyticsTask analyticsTask = (ActivePostsAnalyticsTask) new ActivePostsAnalyticsTask()
                .withStreamingEnvironment(env)
                .withMaxDelay(Time.seconds(600L))
                .withInputStreams(postStream, commentStream, likeStream)
                .withCommentPostMappingConfig(mappingStream, mappingSourceSink)
                .initialize()
                .build()
                .withSink(new TestSink<>());

        try {
            analyticsTask.start();
        } catch (Exception e) {
            fail("Failure in Flink Topology");
        }

        List<PostActivity> results = TestSink.getResults(PostActivity.class);
        for (PostActivity p : results) {
            System.out.println(p.getType());
        }
    }

}
