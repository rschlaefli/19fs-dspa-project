package ch.ethz.infk.dspa.statistics;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.CommentPostMapping;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.stream.helper.SourceSink;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.LikeTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;

public class ActivePostsAnalyticsTaskIT extends AbstractTestBase {

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

		// TODO [rsc] these test data files are not good for this integration test since
		// their event times don't suit each other --> create new test streams for this
		// integration test

		postStream = new PostTestDataGenerator().generate(env, "./../data/test/01_test/post_event_stream.csv",
				maxOutOfOrderness);
		commentStream = new CommentTestDataGenerator().generate(env, "./../data/test/01_test/comment_event_stream.csv",
				maxOutOfOrderness);
		likeStream = new LikeTestDataGenerator().generate(env, "./../data/test/01_test/likes_event_stream.csv",
				maxOutOfOrderness);

		mappingSourceSink = CommentTestDataGenerator
				.generateSourceSink("./../data/test/01_test/comment_event_stream.csv");
		mappingStream = env.addSource(mappingSourceSink);

		TestSink.reset();
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

		List<String> results = TestSink.getResults(String.class);
		results.add(0, "window;postId;type;count");

		// to see all output can also write to file
//		Path file = Paths.get("./test_out.csv");
//		Files.write(file, results, Charset.forName("UTF-8"));

		for (String result : results) {
			System.out.println(result);
		}
	}

}
