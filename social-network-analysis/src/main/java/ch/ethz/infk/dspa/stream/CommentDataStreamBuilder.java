package ch.ethz.infk.dspa.stream;

import java.util.Properties;

import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import ch.ethz.infk.dspa.avro.Comment;

public class CommentDataStreamBuilder extends SocialNetworkDataStreamBuilder<Comment> {

	public CommentDataStreamBuilder(StreamExecutionEnvironment env) {
		super(env);
	}

	@Override
	public DataStream<Comment> build() {

		ensureValidKafkaConfiguration();

		String topic = "comment";
		AvroDeserializationSchema<Comment> avroSchema = AvroDeserializationSchema.forSpecific(Comment.class);
		Properties props = buildKafkaConsumerProperties();

		FlinkKafkaConsumer<Comment> kafkaConsumer = new FlinkKafkaConsumer<>(topic, avroSchema, props);

		DataStream<Comment> commentStream = env.addSource(kafkaConsumer)
				.assignTimestampsAndWatermarks(
						new BoundedOutOfOrdernessTimestampExtractor<Comment>(getMaxOutOfOrderness()) {
							private static final long serialVersionUID = 1L;

							@Override
							public long extractTimestamp(Comment element) {
								return element.getCreationDate().getMillis();
							}
						});

		return commentStream;
	}

}
