package ch.ethz.infk.dspa.stream;

import java.util.Properties;

import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import ch.ethz.infk.dspa.avro.Like;

public class LikeDataStreamBuilder extends SocialNetworkDataStreamBuilder<Like> {

	public LikeDataStreamBuilder(StreamExecutionEnvironment env) {
		super(env);
	}

	@Override
	public DataStream<Like> build() {

		String topic = "like";
		AvroDeserializationSchema<Like> avroSchema = AvroDeserializationSchema.forSpecific(Like.class);
		Properties props = buildKafkaConsumerProperties();

		FlinkKafkaConsumer<Like> kafkaConsumer = new FlinkKafkaConsumer<>(topic, avroSchema, props);

		DataStream<Like> likeStream = env.addSource(kafkaConsumer)
				.assignTimestampsAndWatermarks(
						new BoundedOutOfOrdernessTimestampExtractor<Like>(getMaxOutOfOrderness()) {
							private static final long serialVersionUID = 1L;

							@Override
							public long extractTimestamp(Like element) {
								return element.getCreationDate().getMillis();
							}
						});

		return likeStream;
	}
}
