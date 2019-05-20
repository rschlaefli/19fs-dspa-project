package ch.ethz.infk.dspa.stream.connectors;

import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class KafkaConsumerBuilder<T extends SpecificRecord> {

	private String topic;
	private Class<T> tClass;
	private String groupId;
	private String bootstrapServers;

	public SourceFunction<T> build() {

		if (topic == null || tClass == null || groupId == null || bootstrapServers == null) {
			throw new IllegalArgumentException("All fields must be set");
		}

		AvroDeserializationSchema<T> avroSchema = AvroDeserializationSchema.forSpecific(tClass);

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", bootstrapServers);
		props.setProperty("group.id", groupId);

		FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topic, avroSchema, props);

		// TODO: we might not want to do this
		// TODO: enable checkpointing of these offsets
		kafkaConsumer.setStartFromEarliest();

		return kafkaConsumer;
	}

	public KafkaConsumerBuilder<T> withTopic(String topic) {
		this.topic = topic;
		return this;
	}

	public KafkaConsumerBuilder<T> withKafkaConnection(String bootstrapServers, String groupId) {
		this.bootstrapServers = bootstrapServers;
		this.groupId = groupId;
		return this;
	}

	public KafkaConsumerBuilder<T> withClass(Class<T> tClass) {
		this.tClass = tClass;
		return this;
	}

}
