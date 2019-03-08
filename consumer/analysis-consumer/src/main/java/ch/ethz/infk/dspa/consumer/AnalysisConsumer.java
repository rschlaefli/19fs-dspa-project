package ch.ethz.infk.dspa.consumer;

import java.util.Properties;

import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;

public class AnalysisConsumer {

	String bootstrapServers = "127.0.0.1:9092";
	String groupId = "consumer-group-1";

	public void start() {

		Logger logger = LoggerFactory.getLogger(this.getClass());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Post> postStream = env.addSource(buildPostSource());
		DataStream<Comment> commentStream = env.addSource(buildCommentSource());
		DataStream<Like> likeStream = env.addSource(buildLikeSource());

		postStream.map(post -> "Post: " + post.getId() + " " + post.getCreationDate()).print();
		commentStream.map(comment -> "Comment: " + comment.getId() + " reply to: " + comment.getReplyToPostId())
				.print();
		likeStream.map(like -> "Like: " + like.getPostId()).print();

		// execute program
		try {
			env.execute("Flink Streaming Java API Wikipedia Social Network Analysis");
		} catch (Exception e) {
			// TODO [nku] error handling
			e.printStackTrace();
		}

	}

	private Properties buildProperties() {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", bootstrapServers);
		props.setProperty("group.id", groupId);

		return props;
	}

	private FlinkKafkaConsumer<Post> buildPostSource() {
		String topic = "post";
		AvroDeserializationSchema<Post> avroSchema = AvroDeserializationSchema.forSpecific(Post.class);
		FlinkKafkaConsumer<Post> kafkaConsumer = new FlinkKafkaConsumer<>(topic, avroSchema, buildProperties());

		return kafkaConsumer;
	}

	private FlinkKafkaConsumer<Comment> buildCommentSource() {
		String topic = "comment";
		AvroDeserializationSchema<Comment> avroSchema = AvroDeserializationSchema.forSpecific(Comment.class);
		FlinkKafkaConsumer<Comment> kafkaConsumer = new FlinkKafkaConsumer<>(topic, avroSchema, buildProperties());

		return kafkaConsumer;
	}

	private FlinkKafkaConsumer<Like> buildLikeSource() {
		String topic = "like";
		AvroDeserializationSchema<Like> avroSchema = AvroDeserializationSchema.forSpecific(Like.class);
		FlinkKafkaConsumer<Like> kafkaConsumer = new FlinkKafkaConsumer<>(topic, avroSchema, buildProperties());

		return kafkaConsumer;
	}

}
