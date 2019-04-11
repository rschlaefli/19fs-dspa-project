package ch.ethz.infk.dspa.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.CommentPostMapping;
import ch.ethz.infk.dspa.stream.helper.SourceSink;
import ch.ethz.infk.dspa.stream.helper.CommentCollectSink;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;

public class CommentStreamBuilderWithPostIdTest extends AbstractTestBase {

	private static List<Comment> comments;
	private static Map<Long, Long> map;

	@BeforeAll
	public static void init() {
		CommentTestDataGenerator generator = new CommentTestDataGenerator();
		comments = generator.getComments();
		map = generator.getMap();
	}

	@Test
	public void testBase() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		CommentCollectSink.values.clear();

		// all replies will produce a mapping
		Long mappingCount = comments.stream().filter(c -> c.getReplyToCommentId() != null).count();

		// create a SourceSink that acts both as Sink and Source for the
		// CommentPostMappings (instead of going via Kafka)
		SourceSink mappingSourceSink = new SourceSink(mappingCount);

		Time maxOutOfOrderness = Time.minutes(60);

		// shuffle the comments randomly such that they don't arrive in order
		Collections.shuffle(comments);

		// create artificial streams
		DataStream<Comment> commentStream = env.fromCollection(comments);
		DataStream<CommentPostMapping> mappingStream = env.addSource(mappingSourceSink);

		// build the enriched comment stream
		DataStream<Comment> enrichedCommentStream = new CommentDataStreamBuilder(env)
				.withCommentStream(commentStream)
				.withPostIdEnriched()
				.withCommentPostMappingStream(mappingStream)
				.withCommentPostMappingSink(mappingSourceSink)
				.withMaxOutOfOrderness(maxOutOfOrderness)
				.build();

		// collect the results in the collect sink where they can be checked
		enrichedCommentStream.addSink(new CommentCollectSink());
		env.execute();

		assertEquals(map.size(), CommentCollectSink.values.size(), "Event Count");

		for (Comment comment : CommentCollectSink.values) {
			Long commentId = comment.getId();
			Long postId = comment.getReplyToPostId();
			assertEquals(map.get(commentId), postId, "Comment " + commentId);
		}
	}

}
