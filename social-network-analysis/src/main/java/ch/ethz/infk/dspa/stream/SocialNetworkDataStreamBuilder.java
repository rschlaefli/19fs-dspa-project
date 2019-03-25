package ch.ethz.infk.dspa.stream;

import java.sql.Time;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class SocialNetworkDataStreamBuilder<T> {

	// TODO [nku] refactor server and group id
	private String bootstrapServers = "127.0.0.1:9092";
	private String groupId = "consumer-group-1";

	StreamExecutionEnvironment env;
	private Time maxOutOfOrderness;

	public SocialNetworkDataStreamBuilder(StreamExecutionEnvironment env) {
		this.env = env;
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

	public Time getMaxOutOfOrderness() {
		return maxOutOfOrderness;
	}

}
