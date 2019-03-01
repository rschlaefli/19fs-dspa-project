package ch.eth.dspa;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// define kafka connection props
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		// create a datastream based on the kafka consumer
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer08<>("topic", new SimpleStringSchema(), properties));

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
