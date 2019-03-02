package ch.ethz.dspa.consumer;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class DSPAConsumer {

  public DataStream subscribe(StreamExecutionEnvironment env, String topic) {
    // define kafka connection props
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test");

    // create a datastream based on the kafka consumer
    DataStream<String> stream = env.addSource(new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties));

    return stream;
  }
}
