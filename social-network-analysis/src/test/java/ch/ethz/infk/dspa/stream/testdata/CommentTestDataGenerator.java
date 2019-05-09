package ch.ethz.infk.dspa.stream.testdata;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.stream.helper.SourceSink;

public class CommentTestDataGenerator extends AbstractTestDataGenerator<Comment> {

	@Override
	public DataStream<Comment> addReturnType(SingleOutputStreamOperator<Comment> out) {
		return out.returns(Comment.class);
	}

	@Override
	public AssignerWithPeriodicWatermarks<TestDataPair<Comment>> getTimestampsAndWatermarkAssigner(
			Time maxOutOfOrderness) {
		return new BoundedOutOfOrdernessTimestampExtractor<TestDataPair<Comment>>(maxOutOfOrderness) {
			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(TestDataPair<Comment> pair) {
				return pair.element.getCreationDate().getMillis();
			}
		};
	}

	@Override
	public Comment generateElement() {
		return Comment.newBuilder()
				.setId(1l)
				.setPersonId(2l)
				.setCreationDate(DateTime.now())
				.setLocationIP("locationIP")
				.setBrowserUsed("browserUsed")
				.setContent("content")
				.setReplyToPostId(3l)
				.setReplyToCommentId(4l)
				.setPlaceId(3l)
				.build();
	}

	@Override
	public TestDataPair<Comment> parseLine(String line) {
		String[] parts = line.split("\\|");

		Long commentId = Long.parseLong(parts[0]);
		Long personId = Long.parseLong(parts[1]);
		DateTime creationDate = parseDateTime(parts[2]);
		String locationIP = parts[3];
		String browserUsed = parts[4];
		String content = parts[5];

		Long replyToPostId = null;
		if (StringUtils.isNotEmpty(parts[6])) {
			replyToPostId = Long.parseLong(parts[6]);
		}

		Long replyToCommentId = null;
		if (StringUtils.isNotEmpty(parts[7])) {
			replyToCommentId = Long.parseLong(parts[7]);
		}

		Long placeId = null;
		if (StringUtils.isNotEmpty(parts[8])) {
			placeId = Long.parseLong(parts[8]);
		}

		Comment comment = Comment.newBuilder()
				.setId(commentId)
				.setPersonId(personId)
				.setCreationDate(creationDate)
				.setLocationIP(locationIP)
				.setBrowserUsed(browserUsed)
				.setContent(content)
				.setReplyToPostId(replyToPostId)
				.setReplyToCommentId(replyToCommentId)
				.setPlaceId(placeId)
				.build();

		return TestDataPair.of(comment, null);
	}

	public static SourceSink generateSourceSink(String file) throws Exception {
		// all replies will produce a mapping
		Long mappingCount = new CommentTestDataGenerator().generate(file).stream()
				.filter(c -> c.getReplyToCommentId() != null).count();

		// create a SourceSink that acts both as Sink and Source for the
		// CommentPostMappings (instead of going via Kafka)
		return new SourceSink(mappingCount);
	}
}
