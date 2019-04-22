package ch.ethz.infk.dspa.stream;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public abstract class AbstractDataStreamBuilder<T> {

	private String bootstrapServers;
	private String groupId;

	private Time maxOutOfOrderness;

	StreamExecutionEnvironment env;
	DataStream<T> stream;

	public AbstractDataStreamBuilder(StreamExecutionEnvironment env) {
		this.env = env;
	}

	public abstract DataStream<T> build();

	public AbstractDataStreamBuilder<T> withInputStream(DataStream<T> inputStream) {
		this.stream = inputStream;
		return this;
	}

	public AbstractDataStreamBuilder<T> withKafkaConnection(String bootstrapServers, String groupId) {
		this.bootstrapServers = bootstrapServers;
		this.groupId = groupId;
		return this;
	}

	public AbstractDataStreamBuilder<T> withMaxOutOfOrderness(Time maxOutOfOrderness) {
		this.maxOutOfOrderness = maxOutOfOrderness;
		return this;
	}


	public Properties buildKafkaConsumerProperties() {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", bootstrapServers);
		props.setProperty("group.id", groupId);
		return props;
	}

	public void ensureValidKafkaConfiguration() {
		if (this.bootstrapServers == null || this.groupId == null) {
			throw new IllegalArgumentException("INVALID_KAFKA_CONFIG");
		}
	}

	public Time getMaxOutOfOrderness() {
		return maxOutOfOrderness;
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public String getGroupId() {
		return groupId;
	}

}
