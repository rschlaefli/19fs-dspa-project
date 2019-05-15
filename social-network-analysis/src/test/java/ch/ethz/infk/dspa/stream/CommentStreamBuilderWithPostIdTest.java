package ch.ethz.infk.dspa.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;

public class CommentStreamBuilderWithPostIdTest extends AbstractTestBase {

	private StreamExecutionEnvironment env;
	private Time maxOutOfOrderness;

	@BeforeEach
	public void setup() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		maxOutOfOrderness = Time.hours(1);

		TestSink.reset();
	}

	@Test
	public void testCommentStreamBuilderWithPostId() throws Exception {

		String testFile = "src/test/java/resources/stream/comment_stream.csv";

		DataStream<Comment> commentStream = new CommentTestDataGenerator().generate(env, testFile, maxOutOfOrderness);

		// build the enriched comment stream
		DataStream<Comment> enrichedCommentStream = new CommentDataStreamBuilder(env)
				.withInputStream(commentStream)
				.withPostIdEnriched(Time.hours(1000))
				.withMaxOutOfOrderness(maxOutOfOrderness)
				.build();

		// collect the results in the collect sink where they can be checked
		enrichedCommentStream.addSink(new TestSink<Comment>());
		env.execute();

		Map<Long, List<Comment>> timestampedResults = TestSink.getResultsTimestamped(Comment.class);

		List<Comment> results = timestampedResults.entrySet().stream().map(Entry::getValue).flatMap(List::stream)
				.collect(Collectors.toList());

		Map<Long, Long> map = getCommentToPostMapping(testFile);

		assertEquals(map.size(), results.size(), "Event Count");

		// check for correct mapping
		for (Comment comment : results) {
			Long commentId = comment.getId();
			Long postId = comment.getReplyToPostId();
			assertEquals(map.get(commentId), postId, "Comment " + commentId);
		}

		// check for correct timestamp
		for (Entry<Long, List<Comment>> entry : timestampedResults.entrySet()) {
			Long timestamp = entry.getKey();
			for (Comment comment : entry.getValue()) {
				Long commentId = comment.getId();
				Long postId = comment.getReplyToPostId();

				// check for correct mapping
				assertEquals(map.get(commentId), postId, "Comment " + commentId);

				// check for correct timestamp
				assertEquals(new Long(comment.getCreationDate().getMillis()), timestamp,
						"Timestamp of Comment changed");

			}
		}
	}

	private Map<Long, Long> getCommentToPostMapping(String file) throws IOException {

		Map<Long, Long> map = new HashMap<>();
		List<Comment> comments = new CommentTestDataGenerator().generate(file);

		// sort such that order is in event time
		Collections.sort(comments, new Comparator<Comment>() {
			@Override
			public int compare(Comment o1, Comment o2) {
				return o1.getCreationDate().compareTo(o2.getCreationDate());
			}
		});

		for (Comment comment : comments) {
			Long commentId = comment.getId();
			Long postId;
			if (comment.getReplyToPostId() != null) {
				postId = comment.getReplyToPostId();
			} else if (map.get(comment.getReplyToCommentId()) != null) {
				postId = map.get(comment.getReplyToCommentId());
			} else {
				throw new IllegalArgumentException("The given file does not represent a well formed comment stream");
			}
			map.put(commentId, postId);
		}

		return map;
	}

}
