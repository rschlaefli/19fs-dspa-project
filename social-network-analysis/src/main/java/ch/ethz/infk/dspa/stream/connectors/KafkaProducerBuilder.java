package ch.ethz.infk.dspa.stream.connectors;

import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaProducerBuilder<T> {

	private String topic;
	private Class<T> tClass;
	private String bootstrapServers;
	private ExecutionConfig config;

	public SinkFunction<T> build() {

		if (topic == null || tClass == null || bootstrapServers == null || config == null) {
			throw new IllegalArgumentException("All fields must be set");
		}

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

		TypeInformationSerializationSchema<T> schema = new TypeInformationSerializationSchema<>(
				TypeInformation.of(tClass), config);

		FlinkKafkaProducer<T> kafkaProducer = new FlinkKafkaProducer<>(topic, schema, props);

		return kafkaProducer;
	}

	public KafkaProducerBuilder<T> withTopic(String topic) {
		this.topic = topic;
		return this;
	}

	public KafkaProducerBuilder<T> withKafkaConnection(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
		return this;
	}

	public KafkaProducerBuilder<T> withExecutionConfig(ExecutionConfig config) {
		this.config = config;
		return this;
	}

	public KafkaProducerBuilder<T> withClass(Class<T> tClass) {
		this.tClass = tClass;
		return this;
	}

}
