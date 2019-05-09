package ch.ethz.infk.dspa.stream.testdata;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;

public class FeatureTestDataGenerator extends AbstractTestDataGenerator<Feature> {

	@Override
	public TestDataPair<Feature> parseLine(String line) {

		String[] parts = line.split("\\|");
		DateTime timestamp = parseDateTime(parts[0]);
		Feature.EventType eventType = Feature.EventType.valueOf(parts[1]);
		Long personId = Long.parseLong(parts[2]);
		String eventId = parts[3];

		Feature.FeatureId featureId = Feature.FeatureId.valueOf(parts[4]);
		Double featureValue = Double.parseDouble(parts[5]);

		Feature f = new Feature()
				.withFeatureId(featureId)
				.withFeatureValue(featureValue);

		switch (eventType) {
		case COMMENT:
			Long commentId = Long.parseLong(eventId);
			f = f.withEvent(
					Comment.newBuilder().setId(commentId).setPersonId(personId).setCreationDate(timestamp).build());
			break;
		case LIKE:

			String[] idParts = eventId.split("_");

			Long likePostId = Long.parseLong(idParts[0]);
			Long likePersonId = Long.parseLong(idParts[1]);
			assert (likePersonId == personId);

			f = f.withEvent(
					Like.newBuilder().setPostId(likePostId).setPersonId(personId).setCreationDate(timestamp).build());
			break;
		case POST:
			Long postId = Long.parseLong(eventId);
			f = f.withEvent(Post.newBuilder().setId(postId).setPersonId(personId).setCreationDate(timestamp).build());
			break;
		default:
			throw new IllegalArgumentException("Illegal Feature Event Type");
		}

		return TestDataPair.of(f, timestamp);
	}

	@Override
	public DataStream<Feature> addReturnType(SingleOutputStreamOperator<Feature> out) {
		return out.returns(Feature.class);
	}

	@Override
	public AssignerWithPeriodicWatermarks<TestDataPair<Feature>> getTimestampsAndWatermarkAssigner(
			Time maxOutOfOrderness) {
		return new BoundedOutOfOrdernessTimestampExtractor<TestDataPair<Feature>>(maxOutOfOrderness) {
			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(TestDataPair<Feature> pair) {
				return pair.timestamp.getMillis();
			}
		};
	}

	@Override
	public Feature generateElement() {
		return new Feature()
				.withEvent(Post.newBuilder().setId(1L).setPersonId(2L).setCreationDate(DateTime.now()).build())
				.withFeatureId(Feature.FeatureId.CONTENTS_SHORT)
				.withFeatureValue(10.0);
	}

}
