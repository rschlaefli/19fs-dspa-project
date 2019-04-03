package ch.ethz.infk.dspa.stream;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public abstract class SocialNetworkDataStreamBuilder<T> {

	private String bootstrapServers;
	private String groupId;

	StreamExecutionEnvironment env;
	private Time maxOutOfOrderness;

	public SocialNetworkDataStreamBuilder(StreamExecutionEnvironment env) {
		this.env = env;
	}

	public SocialNetworkDataStreamBuilder<T> withKafkaConnection(String bootstrapServers, String groupId) {
		this.bootstrapServers = bootstrapServers;
		this.groupId = groupId;
		return this;
	}

	public SocialNetworkDataStreamBuilder<T> withMaxOutOfOrderness(Time maxOutOfOrderness) {
		this.maxOutOfOrderness = maxOutOfOrderness;
		return this;
	}

	public abstract DataStream<T> build();

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

}
