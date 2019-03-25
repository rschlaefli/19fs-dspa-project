package ch.ethz.infk.dspa.stream;

import java.util.Properties;

import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import ch.ethz.infk.dspa.avro.Post;

public class PostDataStreamBuilder extends SocialNetworkDataStreamBuilder<Post> {

	public PostDataStreamBuilder(StreamExecutionEnvironment env) {
		super(env);
	}

	@Override
	public DataStream<Post> build() {

		String topic = "post";
		AvroDeserializationSchema<Post> avroSchema = AvroDeserializationSchema.forSpecific(Post.class);
		Properties props = buildKafkaConsumerProperties();

		FlinkKafkaConsumer<Post> kafkaConsumer = new FlinkKafkaConsumer<>(topic, avroSchema, props);

		DataStream<Post> postStream = env.addSource(kafkaConsumer)
				.assignTimestampsAndWatermarks(
						new BoundedOutOfOrdernessTimestampExtractor<Post>(getMaxOutOfOrderness()) {
							private static final long serialVersionUID = 1L;

							@Override
							public long extractTimestamp(Post element) {
								return element.getCreationDate().getMillis();
							}
						});

		return postStream;
	}

}
