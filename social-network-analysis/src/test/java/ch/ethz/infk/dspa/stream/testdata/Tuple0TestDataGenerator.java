package ch.ethz.infk.dspa.stream.testdata;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

public class Tuple0TestDataGenerator extends AbstractTestDataGenerator<Tuple0> {

	@Override
	public Tuple0 generateElement() {
		return new Tuple0();
	}

	@Override
	public AssignerWithPeriodicWatermarks<TestDataPair<Tuple0>> getTimestampsAndWatermarkAssigner(
			Time maxOutOfOrderness) {

		return new BoundedOutOfOrdernessTimestampExtractor<TestDataPair<Tuple0>>(maxOutOfOrderness) {
			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(TestDataPair<Tuple0> pair) {
				return pair.getTimestamp().getMillis();
			}
		};
	}

	@Override
	public TestDataPair<Tuple0> parseLine(String line) {

		DateTime timestamp = parseDateTime(line);
		return TestDataPair.of(new Tuple0(), timestamp);
	}

	@Override
	public DataStream<Tuple0> addReturnType(SingleOutputStreamOperator<Tuple0> out) {
		return out.returns(Tuple0.class);
	}

}
