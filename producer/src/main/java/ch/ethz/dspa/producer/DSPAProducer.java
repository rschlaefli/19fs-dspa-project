package ch.ethz.dspa.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DSPAProducer {

	private final static Logger LOG = LogManager.getLogger();

	private ScheduledExecutorService scheduledExecutorService;
	private int numWorkers;

	private Producer<String, String> producer;
	private String file;
	private String topic;

	private long speedup;
	private Duration maxDelay;
	private Duration maxRandomDelay;

	private Random rand;
	private long seed;

	public void start() {

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {

			br.readLine(); // skip header
			String line;
			Duration baseOffset = getBaseOffset();
			long id = 0;

			while ((line = br.readLine()) != null) {

				// parse line
				LocalDateTime creationDate = getCreationDate(line);

				// build runnable
				ProducerTask task = new ProducerTask().withProducer(producer).withId(id).withTopic(topic).withValue(line);

				// calculate delay
				Duration randomDelay = Duration.ofMillis(rand.nextInt((int) maxRandomDelay.toMillis()));
				Duration delay = getDelay(creationDate, baseOffset, randomDelay, speedup);

				// if task is far in the future, delay scheduling
				if (delay.compareTo(maxDelay) > 0) {

					TimeUnit.MILLISECONDS.sleep(delay.minus(maxDelay).toMillis());

					// recalculate delay for scheduling
					delay = getDelay(creationDate, baseOffset, randomDelay, speedup);

				}

				// schedule task
				scheduledExecutorService.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);

				LOG.info("Task {} Scheduled: Offset = {} ms", id, delay.toMillis());
				id++;
			}
		} catch (InterruptedException | IOException e) {
			// TODO [nku] add error handling
		}
		try {
			scheduledExecutorService.shutdown();
			scheduledExecutorService.awaitTermination(3600, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO [nku] add error handling
			e.printStackTrace();
		}
		producer.flush();
		producer.close();

	}

	private Duration getDelay(LocalDateTime creationDate, Duration baseOffset, Duration randomDelay, long speedup) {
		creationDate = creationDate.plus(baseOffset);
		Duration offset = Duration.between(LocalDateTime.now(), creationDate).plus(randomDelay).dividedBy(speedup);

		return offset;
	}

	private LocalDateTime getCreationDate(String line) {
		String creationDateStr = line.split("\\|")[2];
		LocalDateTime creationDate = ZonedDateTime.parse(creationDateStr).toLocalDateTime();
		return creationDate;
	}

	private Duration getBaseOffset() {
		LocalDateTime baseDate = null;
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			reader.readLine(); // skip header
			String line = reader.readLine();
			baseDate = getCreationDate(line);

		} catch (IOException e) {
			// TODO [nku] implement error handling
		}
		return Duration.between(baseDate, LocalDateTime.now());
	}

	private Properties buildProperties() {
		Properties props = new Properties();

		// where is kafka located?
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

		// what types of values are we sending? how to convert to bytes?
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return props;
	}

	public DSPAProducer withMaxRandomDelay(Duration maxRandomDelay) {
		this.maxRandomDelay = maxRandomDelay;
		return this;
	}

	public DSPAProducer withMaxDelay(Duration maxDelay) {
		this.maxDelay = maxDelay;
		return this;
	}

	public DSPAProducer withSeed(long seed) {
		this.seed = seed;
		return this;
	}

	public DSPAProducer withFile(String file) {
		this.file = file;
		return this;
	}

	public DSPAProducer withSpeedup(long speedup) {
		this.speedup = speedup;
		return this;
	}

	public DSPAProducer withTopic(String topic) {
		this.topic = topic;
		return this;
	}

	public DSPAProducer withWorker(int numWorkers) {
		this.numWorkers = numWorkers;
		return this;
	}

	public DSPAProducer create() {
		scheduledExecutorService = Executors.newScheduledThreadPool(numWorkers);

		Properties props = buildProperties();
		producer = new KafkaProducer<>(props);

		rand = new Random(seed);

		return this;
	}

	public class ProducerTask implements Runnable {

		long id;
		Producer<String, String> producer;
		String topic;
		String value;

		@Override
		public void run() {

			if (producer == null || topic == null || value == null) {
				throw new IllegalArgumentException();
			}

			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO [nku] on completion handling of task
					LOG.info("Task {} Completed", id);
				}
			});
		}

		public ProducerTask withId(long id) {
			this.id = id;
			return this;
		}

		public ProducerTask withTopic(String topic) {
			this.topic = topic;
			return this;
		}

		public ProducerTask withValue(String value) {
			this.value = value;
			return this;
		}

		public ProducerTask withProducer(Producer<String, String> producer) {
			this.producer = producer;
			return this;
		}
	}

}
