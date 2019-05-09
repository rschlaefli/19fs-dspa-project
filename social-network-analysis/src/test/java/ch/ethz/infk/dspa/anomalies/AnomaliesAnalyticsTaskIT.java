package ch.ethz.infk.dspa.anomalies;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.FraudulentUser;
import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.CommentPostMapping;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.stream.helper.SourceSink;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.LikeTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

public class AnomaliesAnalyticsTaskIT extends AbstractTestBase {

    private StreamExecutionEnvironment env;
    private DataStream<Post> postStream;
    private DataStream<Comment> commentStream;
    private DataStream<Like> likeStream;

    private SourceSink mappingSourceSink;
    private DataStream<CommentPostMapping> mappingStream;

    @BeforeEach
    public void setup() throws Exception {
        Time maxOutOfOrderness = Time.hours(1);

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        postStream = new PostTestDataGenerator().generate(env, "./../data/test/01_test/post_event_stream.csv", maxOutOfOrderness);
        commentStream = new CommentTestDataGenerator().generate(env, "./../data/test/01_test/comment_event_stream.csv", maxOutOfOrderness);
        likeStream = new LikeTestDataGenerator().generate(env, "./../data/test/01_test/likes_event_stream.csv", maxOutOfOrderness);

        mappingSourceSink = CommentTestDataGenerator.generateSourceSink("./../data/test/01_test/comment_event_stream.csv");
        mappingStream = env.addSource(mappingSourceSink);
    }

    @Test
    public void testAnomaliesConsumer() throws Exception {
        AnomaliesAnalyticsTask analyticsTask = (AnomaliesAnalyticsTask) new AnomaliesAnalyticsTask()
                .withStreamingEnvironment(env)
                .withMaxDelay(Time.seconds(600L))
                .withInputStreams(postStream, commentStream, likeStream)
                .withCommentPostMappingConfig(mappingStream, mappingSourceSink)
                .initialize()
                .build()
                .withSink(new TestSink<>());

        analyticsTask.start();

		List<FraudulentUser> results = TestSink.getResults(FraudulentUser.class);
		for (FraudulentUser f : results) {
			System.out.println(f);
		}
	}
}
