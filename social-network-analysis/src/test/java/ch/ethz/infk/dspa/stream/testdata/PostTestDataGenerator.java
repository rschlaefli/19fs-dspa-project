package ch.ethz.infk.dspa.stream.testdata;

import java.util.Collections;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

import ch.ethz.infk.dspa.avro.Post;

public class PostTestDataGenerator extends AbstractTestDataGenerator<Post> {

	@Override
	public DataStream<Post> addReturnType(SingleOutputStreamOperator<Post> out) {
		return out.returns(Post.class);
	}

	@Override
	public AssignerWithPeriodicWatermarks<TestDataPair<Post>> getTimestampsAndWatermarkAssigner(
			Time maxOutOfOrderness) {
		return new BoundedOutOfOrdernessTimestampExtractor<TestDataPair<Post>>(maxOutOfOrderness) {
			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(TestDataPair<Post> pair) {
				return pair.element.getCreationDate().getMillis();
			}
		};
	}

	@Override
	public Post generateElement() {
		Post post = Post.newBuilder()
				.setId(1l)
				.setPersonId(2l)
				.setCreationDate(DateTime.now())
				.setImageFile("imageFile")
				.setLocationIP("locationIp")
				.setBrowserUsed("browserUsed")
				.setLanguage("language")
				.setContent("content")
				.setTags(Collections.singletonList(3l))
				.setPlaceId(3l)
				.setForumId(4l)
				.build();

		return post;
	}

	@Override
	public TestDataPair<Post> parseLine(String line) {
		String[] parts = line.split("\\|");

		Long postId = Long.parseLong(parts[0]);
		Long personId = Long.parseLong(parts[1]);
		DateTime creationDate = parseDateTime(parts[2]);
		String imageFile = parts[3];
		String locationIP = parts[4];
		String browserUsed = parts[5];
		String language = parts[6];
		String content = parts[7];
		List<Long> tags = parseLongList(parts[8]);
		Long forumId = Long.parseLong(parts[9]);
		Long placeId = Long.parseLong(parts[10]);

		Post post = Post.newBuilder()
				.setId(postId)
				.setPersonId(personId)
				.setCreationDate(creationDate)
				.setImageFile(imageFile)
				.setLocationIP(locationIP)
				.setBrowserUsed(browserUsed)
				.setLanguage(language)
				.setContent(content)
				.setTags(tags)
				.setForumId(forumId)
				.setPlaceId(placeId)
				.build();

		return TestDataPair.of(post, null);

	}
}
