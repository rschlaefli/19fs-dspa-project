package ch.ethz.infk.dspa.recommendations;

import ch.ethz.infk.dspa.helper.Config;
import org.apache.commons.configuration2.Configuration;
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

public class RecommendationsAnalyticsTaskIT extends AbstractTestBase {

	private Configuration config;
	private StreamExecutionEnvironment env;
	private DataStream<Post> postStream;
	private DataStream<Comment> commentStream;
	private DataStream<Like> likeStream;

	private SourceSink mappingSourceSink;
	private DataStream<CommentPostMapping> mappingStream;

	@BeforeEach
	public void setup() throws Exception {
		final Time maxOutOfOrderness = Time.hours(1);

		config = Config.getConfig("src/main/java/ch/ethz/infk/dspa/config.properties");
		env = StreamExecutionEnvironment.getExecutionEnvironment();

		postStream = new PostTestDataGenerator()
				.generate(env, "src/test/java/resources/post_stream.csv", maxOutOfOrderness);
		commentStream = new CommentTestDataGenerator()
				.generate(env, "src/test/java/resources/comment_stream.csv", maxOutOfOrderness);
		likeStream = new LikeTestDataGenerator()
				.generate(env, "src/test/java/resources/like_stream.csv", maxOutOfOrderness);

		mappingSourceSink = CommentTestDataGenerator.generateSourceSink("src/test/java/resources/comment_stream.csv");
		mappingStream = env.addSource(mappingSourceSink);
	}

	@Test
	public void testRecommendationsConsumer() throws Exception {
		RecommendationsAnalyticsTask analyticsTask = (RecommendationsAnalyticsTask) new RecommendationsAnalyticsTask()
				.withPropertiesConfiguration(config)
				.withStreamingEnvironment(env)
				.withStaticFilePath("src/test/java/resources/recommendations/relations/")
				.withMaxDelay(Time.seconds(600L))
				.withInputStreams(postStream, commentStream, likeStream)
				.withCommentPostMappingConfig(mappingStream, mappingSourceSink)
				.initialize()
				.build()
				.withSink(new TestSink<>());

		analyticsTask.toStringStream().print();

		analyticsTask.start();

		/*
		 * List<List<PersonSimilarity>> results = TestSink.getResults(List.class); for (PostActivity p :
		 * results) { System.out.println(p.getType()); }
		 */
	}

}
