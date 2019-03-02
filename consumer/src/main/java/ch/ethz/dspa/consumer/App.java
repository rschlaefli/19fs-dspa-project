package ch.ethz.dspa.consumer;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class App {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // initialize a new consumer
        DSPAConsumer consumer = new DSPAConsumer();

        // subscribe to test_topic
        DataStream stream = consumer.subscribe(env, "post");

        // streaming pipeline
        stream.print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
