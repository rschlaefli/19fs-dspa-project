package ch.uzh.ifi.kafkacourse;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // create producer properties
        Properties props = new Properties();

        // where is kafka located?
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // what types of values are we sending? how to convert to bytes?
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        // key: String, value: String
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // create a record to be sent
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test_topic", "hello world");

        // send data (asynchronously)
        producer.send(record);

        // force all records to be sent synchronously
        producer.flush();

        producer.close();
    }
}
