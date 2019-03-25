package ch.ethz.infk.dspa.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProducerTask implements Runnable {

	private final static Logger LOG = LogManager.getLogger();

	private long id;
	private Producer<String, byte[]> producer;
	private ProducerRecord<String, byte[]> record;

	private ProducerTask() {
	}

	@Override
	public void run() {
		producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO [nku] on completion handling of task
				LOG.info("Task {} Completed", id);
			}
		});
	}

	public static class Builder {

		private Long id = null;
		private Producer<String, byte[]> producer = null;
		private String topic = null;
		private byte[] value = null;

		public Builder withId(long id) {
			this.id = id;
			return this;
		}

		public Builder withTopic(String topic) {
			this.topic = topic;
			return this;
		}

		public Builder withValue(byte[] value) {
			this.value = value;
			return this;
		}

		public Builder withProducer(Producer<String, byte[]> producer) {
			this.producer = producer;
			return this;
		}

		public ProducerTask build() {

			assert (this.id != null);
			assert (this.producer != null);
			assert (this.topic != null);
			assert (this.value != null);

			ProducerTask task = new ProducerTask();
			task.id = this.id;
			task.producer = this.producer;
			task.record = new ProducerRecord<String, byte[]>(topic, value);

			return task;

		}

	}
}
