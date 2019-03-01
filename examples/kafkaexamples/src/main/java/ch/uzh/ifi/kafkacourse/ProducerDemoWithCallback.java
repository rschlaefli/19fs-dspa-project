package ch.uzh.ifi.kafkacourse;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger("debug");

        // create producer properties
        Properties props = new Properties();

        // where is kafka located?
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        // what types of values are we sending? how to convert to bytes?
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // ensure that records are sent instantly
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        // create producer
        // key: String, value: String
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                logger.error("Thread interrupted...");
            }

            // create a record to be sent
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test_topic", "hello " + i);

            // send data (asynchronously)
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute everytime a record is sent or an exception is thrown
                    if (e == null) {
                        // successfully sent
                        logger.info("whatever");
                    } else {
                        // error was thrown
                        logger.error("failed");
                    }
                }
            });
        }

        // force all records to be sent synchronously
        producer.flush();

        producer.close();
    }
}
