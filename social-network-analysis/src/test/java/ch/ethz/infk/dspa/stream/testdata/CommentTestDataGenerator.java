package ch.ethz.infk.dspa.stream.testdata;

import java.time.ZonedDateTime;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

import ch.ethz.infk.dspa.avro.Comment;

public class CommentTestDataGenerator extends TestDataGenerator<Comment> {

	@Override
	public AssignerWithPeriodicWatermarks<Comment> getTimestampsAndWatermarkAssigner(Time maxOutOfOrderness) {
		return new BoundedOutOfOrdernessTimestampExtractor<Comment>(maxOutOfOrderness) {
			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(Comment element) {
				return element.getCreationDate().getMillis();
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
	public Comment parseLine(String line) {
		String[] parts = line.split("\\|");

		Long commentId = Long.parseLong(parts[0]);
		Long personId = Long.parseLong(parts[1]);
		DateTime creationDate = new DateTime(ZonedDateTime.parse(parts[2]).toInstant().toEpochMilli());
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

		return comment;
	}
}
