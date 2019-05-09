package ch.ethz.infk.dspa.stream.testdata;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;

public class PersonSimilarityTestDataGenerator
		extends AbstractTestDataGenerator<PersonSimilarity> {

	@Override
	public DataStream<PersonSimilarity> addReturnType(SingleOutputStreamOperator<PersonSimilarity> out) {
		return out.returns(PersonSimilarity.class);
	}

	@Override
	public AssignerWithPeriodicWatermarks<TestDataPair<PersonSimilarity>> getTimestampsAndWatermarkAssigner(
			Time maxOutOfOrderness) {
		return new BoundedOutOfOrdernessTimestampExtractor<TestDataPair<PersonSimilarity>>(maxOutOfOrderness) {
			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(TestDataPair<PersonSimilarity> pair) {
				return pair.timestamp.getMillis();
			}
		};
	}

	@Override
	public PersonSimilarity generateElement() {
		return new PersonSimilarity()
				.withPerson1Id(1L)
				.withPerson2Id(2L)
				.withSimilarity(0.7);
	}

	@Override
	public TestDataPair<PersonSimilarity> parseLine(String line) {
		String[] parts = line.split("\\|");

		PersonSimilarity similarity = new PersonSimilarity()
				.withPerson1Id(Long.parseLong(parts[0]))
				.withPerson2Id(Long.parseLong(parts[1]))
				.withSimilarity(Double.parseDouble(parts[2]));

		DateTime creationDate = parseDateTime(parts[3]);

		return TestDataPair.of(similarity, creationDate);
	}

}
