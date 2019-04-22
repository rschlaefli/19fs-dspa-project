package ch.ethz.infk.dspa.stream;

import ch.ethz.infk.dspa.avro.Post;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class PostDataStreamBuilder extends AbstractDataStreamBuilder<Post> {

    public PostDataStreamBuilder(StreamExecutionEnvironment env) {
        super(env);
    }

    @Override
    public PostDataStreamBuilder withInputStream(DataStream<Post> inputStream) {
        super.withInputStream(inputStream);
        return this;
    }

    @Override
    public DataStream<Post> build() {

        if (this.stream == null) {
            ensureValidKafkaConfiguration();

            String topic = "post";
            AvroDeserializationSchema<Post> avroSchema = AvroDeserializationSchema.forSpecific(Post.class);
            Properties props = buildKafkaConsumerProperties();

            FlinkKafkaConsumer<Post> kafkaConsumer = new FlinkKafkaConsumer<>(topic, avroSchema, props);

            this.stream = env.addSource(kafkaConsumer);
        }

        this.stream = this.stream
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Post>(getMaxOutOfOrderness()) {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public long extractTimestamp(Post element) {
                                return element.getCreationDate().getMillis();
                            }
                        });

        return this.stream;
    }
}
