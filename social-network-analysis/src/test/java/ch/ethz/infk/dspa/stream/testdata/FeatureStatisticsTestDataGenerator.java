package ch.ethz.infk.dspa.stream.testdata;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;
import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;

public class FeatureStatisticsTestDataGenerator extends AbstractTestDataGenerator<FeatureStatistics> {

	@Override
	public TestDataPair<FeatureStatistics> parseLine(String line) {

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

		Double mean = Double.parseDouble(parts[6]);
		Double stdDev = Double.parseDouble(parts[7]);

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
		FeatureStatistics stats = new FeatureStatistics(f);
		stats.setMean(mean);
		stats.setStdDev(stdDev);

		return TestDataPair.of(stats, timestamp);
	}

	@Override
	public DataStream<FeatureStatistics> addReturnType(SingleOutputStreamOperator<FeatureStatistics> out) {
		return out.returns(FeatureStatistics.class);
	}

	@Override
	public AssignerWithPeriodicWatermarks<TestDataPair<FeatureStatistics>> getTimestampsAndWatermarkAssigner(
			Time maxOutOfOrderness) {
		return new BoundedOutOfOrdernessTimestampExtractor<TestDataPair<FeatureStatistics>>(maxOutOfOrderness) {
			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(TestDataPair<FeatureStatistics> pair) {
				return pair.timestamp.getMillis();
			}
		};
	}

	@Override
	public FeatureStatistics generateElement() {
		Feature feature = new Feature()
				.withEvent(Post.newBuilder().setId(1L).setPersonId(2L).setCreationDate(DateTime.now()).build())
				.withFeatureId(Feature.FeatureId.CONTENTS_SHORT)
				.withFeatureValue(10.0);
		FeatureStatistics stats = new FeatureStatistics(feature);
		stats.setMean(10.0);
		stats.setStdDev(2.0);
		return stats;
	}

}
