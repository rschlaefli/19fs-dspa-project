package ch.ethz.infk.dspa.stream.testdata;

import java.time.ZonedDateTime;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

import ch.ethz.infk.dspa.avro.Like;

public class LikeTestDataGenerator extends AbstractTestDataGenerator<Like> {

	@Override
	public AssignerWithPeriodicWatermarks<TestDataPair<Like>> getTimestampsAndWatermarkAssigner(
			Time maxOutOfOrderness) {
		return new BoundedOutOfOrdernessTimestampExtractor<TestDataPair<Like>>(maxOutOfOrderness) {
			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(TestDataPair<Like> pair) {
				return pair.element.getCreationDate().getMillis();
			}
		};
	}

	@Override
	public Like generateElement() {
		return Like.newBuilder().setPersonId(1l).setPostId(2l).setCreationDate(DateTime.now()).build();
	}

	@Override
	public TestDataPair<Like> parseLine(String line) {
		String[] parts = line.split("\\|");

		Long personId = Long.parseLong(parts[0]);
		Long postId = Long.parseLong(parts[1]);
		DateTime creationDate = new DateTime(ZonedDateTime.parse(parts[2]).toInstant().toEpochMilli());

		Like like = Like.newBuilder()
				.setPersonId(personId)
				.setPostId(postId)
				.setCreationDate(creationDate)
				.build();

		return TestDataPair.of(like, null);
	}
}