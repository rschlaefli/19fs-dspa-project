package ch.ethz.infk.dspa.stream.connectors;

import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaProducerBuilder {

	private String topic;
	private String bootstrapServers;
	private ExecutionConfig config;

	public SinkFunction<String> build() {

		if (topic == null || bootstrapServers == null || config == null) {
			throw new IllegalArgumentException("All fields must be set");
		}

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

		FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(topic, new SimpleStringSchema(), props);

		return kafkaProducer;
	}

	public KafkaProducerBuilder withTopic(String topic) {
		this.topic = topic;
		return this;
	}

	public KafkaProducerBuilder withKafkaConnection(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
		return this;
	}

	public KafkaProducerBuilder withExecutionConfig(ExecutionConfig config) {
		this.config = config;
		return this;
	}
}
