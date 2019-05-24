package ch.ethz.infk.dspa.producer;

import java.time.Duration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class App {
	public static void main(String[] args) {

		Options options = buildOptions();

		try {

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			Long seed = cmd.getOptionValue("seed") != null ? Long.parseLong(cmd.getOptionValue("seed")) : null;

			StreamProducer producer = new StreamProducer.Builder()
					.withWorker(Integer.parseInt(cmd.getOptionValue("worker", "2")))
					.withFile(cmd.getOptionValue("file")).withTopic(cmd.getOptionValue("topic"))
					.withSchema(cmd.getOptionValue("schema"))
					.withStartBase(cmd.getOptionValue("start"))
					.withSpeedup(Long.parseLong(cmd.getOptionValue("speedup")))
					.withMaxSchedulingDelay(Duration.ofSeconds(Long.parseLong(cmd.getOptionValue("sdelay"))))
					.withMaxRandomDelay(Duration.ofSeconds(Long.parseLong(cmd.getOptionValue("rdelay"))))
					.withKafkaServer(cmd.getOptionValue("kafkaserver"))
					.withSeed(seed).build();

			producer.start();

		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	private static Options buildOptions() {
		Options options = new Options();

		options.addOption(Option.builder("file").hasArg().required().type(String.class)
				.desc("path to streaming file").build());

		options.addOption(Option.builder("schema").hasArg().required().type(String.class)
				.desc("path to avro schema file").build());

		options.addOption(
				Option.builder("topic").hasArg().required().type(String.class).desc("kafka topic").build());

		options.addOption(Option.builder("kafkaserver").hasArg().required().type(String.class)
				.desc("kafka server").build());

		options.addOption(
				Option.builder("speedup").hasArg().required().type(Integer.class).desc("stream speedup").build());

		options.addOption(
				Option.builder("rdelay").hasArg().required().type(Integer.class).desc("max random delay (sec)")
						.build());

		options.addOption(Option.builder("sdelay").hasArg().required().type(Integer.class)
				.desc("max scheduling delay (sec)").build());

		options.addOption(Option.builder("seed").hasArg().type(Long.class).desc("random seed").build());

		options.addOption(Option.builder("worker").hasArg().type(Integer.class)
				.desc("number of threads executing scheduled tasks").build());

		options.addOption(Option.builder("start").hasArg().type(String.class)
				.desc("start timestamp (base reference for event times)").build());

		return options;
	}
}
