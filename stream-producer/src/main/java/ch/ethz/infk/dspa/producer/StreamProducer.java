package ch.ethz.infk.dspa.producer;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StreamProducer {

	private final static Logger LOG = LogManager.getLogger();

	private ScheduledExecutorService scheduledExecutorService;

	private String file;
	private Schema schema;

	private Map<String, String> fieldNameMap;

	private Producer<String, byte[]> producer;
	private String topic;

	private long speedup;
	private Duration maxSchedulingDelay;
	private Duration maxRandomDelay;

	private Random rand;

	public void start() {

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {

			String header = br.readLine();
			String[] fieldNames = header.split("\\|");

			String line;

			LocalDateTime baseReferenceDateTime = getBaseReferenceDateTime();
			LocalDateTime baseStartDateTime = LocalDateTime.now();

			long id = 0;

			while ((line = br.readLine()) != null) {

				// parse line
				LocalDateTime creationDateTime = getCreationDate(line);
				GenericRecord record = parseLine(line, fieldNames, schema);

				// build runnable
				ProducerTask task = new ProducerTask.Builder().withId(id).withProducer(producer).withTopic(topic)
						.withValue(serialize(record)).build();

				// calculate delay
				Duration randomDelay = Duration.ofMillis(rand.nextInt((int) maxRandomDelay.toMillis()));

				LocalDateTime schedulingDateTime = getSchedulingDateTime(baseReferenceDateTime, creationDateTime,
						baseStartDateTime, randomDelay, speedup);

				// if task is far in the future, delay scheduling
				Duration delay = Duration.between(LocalDateTime.now(), schedulingDateTime);
				if (delay.compareTo(maxSchedulingDelay) > 0) {
					// wait until within max delay
					TimeUnit.MILLISECONDS.sleep(delay.minus(maxSchedulingDelay).toMillis());
					// update delay
					delay = Duration.between(LocalDateTime.now(), schedulingDateTime);
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
			scheduledExecutorService.awaitTermination(maxSchedulingDelay.getSeconds(), TimeUnit.SECONDS);

		} catch (InterruptedException e) {
			// TODO [nku] add error handling
			e.printStackTrace();
		}
		producer.flush();
		producer.close();
	}

	private LocalDateTime getSchedulingDateTime(LocalDateTime baseReferenceDateTime, LocalDateTime creationDateTime,
			LocalDateTime baseStartDateTime, Duration randomDelay, long speedup) {

		Duration offset = Duration.between(baseReferenceDateTime, creationDateTime) // difference to first event
				.plus(randomDelay) // add bounded random delay
				.dividedBy(speedup); // calculate speedup

		return baseStartDateTime.plus(offset);
	}

	private GenericRecord parseLine(String line, String[] fieldNames, Schema schema) {
		GenericRecord record = new GenericData.Record(schema);

		String[] parts = line.split("\\|");
		for (int i = 0; i < fieldNames.length; i++) {

			Schema fieldSchema = schema.getField(fieldNameMap.get(fieldNames[i])).schema();

			Type type = fieldSchema.getType();
			LogicalType logicalType = fieldSchema.getLogicalType();

			// TODO [nku] refactor code

			Object o;
			if (logicalType != null && logicalType.getName().equals("timestamp-millis")) {
				o = ZonedDateTime.parse(parts[i]).toInstant().toEpochMilli();
			} else {
				switch (type) {
				case LONG:
					if (StringUtils.isEmpty(parts[i])) {
						o = -1l;
					} else {
						o = Long.parseLong(parts[i]);
					}
					break;

				case ARRAY:
					if (StringUtils.isEmpty(parts[i]) || parts[i].equals("[]")) {
						o = new ArrayList<String>();
					} else {
						String tagArrayString = parts[i].replace("[", "");
						tagArrayString = tagArrayString.replace("]", "");
						List<String> tagIdsAsStringList = Arrays.asList(tagArrayString.split(","));
						List<Long> tagIdsAsLongList = tagIdsAsStringList.stream().map(tagId -> Long.valueOf(tagId))
								.collect(Collectors.toList());
						o = tagIdsAsLongList;
					}
					break;

				default:
					o = parts[i];
				}
			}

			record.put(fieldNameMap.get(fieldNames[i]), o);
		}

		return record;
	}

	private LocalDateTime getCreationDate(String line) {
		String creationDateStr = line.split("\\|")[2];
		LocalDateTime creationDate = ZonedDateTime.parse(creationDateStr).toLocalDateTime();
		return creationDate;
	}

	private LocalDateTime getBaseReferenceDateTime() {
		LocalDateTime baseDate = null;
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			reader.readLine(); // skip header
			String line = reader.readLine();
			baseDate = getCreationDate(line);

		} catch (IOException e) {
			// TODO [nku] implement error handling
		}
		return baseDate;
	}

	private byte[] serialize(GenericRecord record) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {

			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
			writer.write(record, encoder);
			encoder.flush();
			out.close();

		} catch (IOException e) {
			// TODO [nku] add error handling
			e.printStackTrace();
		}

		return out.toByteArray();
	}

	public static class Builder {

		private Integer numWorkers = null;

		private String file;
		private String schema;

		private String topic;
		private String kafkaServer;

		private Long speedup;
		private Duration maxSchedulingDelay;
		private Duration maxRandomDelay;

		private Long seed;

		public Builder withMaxRandomDelay(Duration maxRandomDelay) {
			this.maxRandomDelay = maxRandomDelay;
			return this;
		}

		public Builder withMaxSchedulingDelay(Duration maxSchedulingDelay) {
			this.maxSchedulingDelay = maxSchedulingDelay;
			return this;
		}

		public Builder withSeed(Long seed) {
			this.seed = seed;
			return this;
		}

		public Builder withFile(String file) {
			this.file = file;
			return this;
		}

		public Builder withKafkaServer(String kafkaServer) {
			this.kafkaServer = kafkaServer;
			return this;
		}

		public Builder withSpeedup(long speedup) {
			this.speedup = speedup;
			return this;
		}

		public Builder withTopic(String topic) {
			this.topic = topic;
			return this;
		}

		public Builder withSchema(String schema) {
			this.schema = schema;
			return this;
		}

		public Builder withWorker(int numWorkers) {
			this.numWorkers = numWorkers;
			return this;
		}

		public StreamProducer build() {

			StreamProducer streamProducer = new StreamProducer();

			// build scheduled executor service
			if (numWorkers == null) {
				numWorkers = 1;
			}
			streamProducer.scheduledExecutorService = Executors.newScheduledThreadPool(numWorkers);

			// build schema & check streaming file is specified
			try {
				streamProducer.schema = new Schema.Parser().parse(new File(this.schema));

				streamProducer.fieldNameMap = new HashMap<>();

				streamProducer.schema.getFields().forEach(field -> {
					streamProducer.fieldNameMap.put(field.name(), field.name());
					field.aliases().forEach(alias -> streamProducer.fieldNameMap.put(alias, field.name()));
				});

			} catch (IOException e) {
				// TODO [nku] implement error handling
				e.printStackTrace();
			}

			assert (StringUtils.isNotEmpty(this.file));
			streamProducer.file = this.file;

			// build kafka producer
			Properties props = new Properties();

			props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaServer);
			props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

			streamProducer.producer = new KafkaProducer<>(props);

			assert (StringUtils.isNotEmpty(topic));
			streamProducer.topic = this.topic;

			// build random
			streamProducer.rand = new Random(seed);

			// configure stream producer
			if (this.speedup == null) {
				this.speedup = 1l;
			}
			streamProducer.speedup = this.speedup;

			if (this.maxRandomDelay == null) {
				this.maxRandomDelay = Duration.ZERO;
			}
			streamProducer.maxRandomDelay = this.maxRandomDelay;

			if (this.maxSchedulingDelay == null) {
				this.maxSchedulingDelay = Duration.ofHours(1);
			}
			streamProducer.maxSchedulingDelay = this.maxSchedulingDelay;

			return streamProducer;
		}

	}

}
