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
					.withSchema(cmd.getOptionValue("schema")).withKafkaServer(cmd.getOptionValue("server"))
					.withSpeedup(Duration.ofSeconds(Long.parseLong(cmd.getOptionValue("speedup", "1"))).toMillis())
					.withMaxSchedulingDelay(Duration.ofSeconds(Long.parseLong(cmd.getOptionValue("sdelay", "3600"))))
					.withMaxRandomDelay(Duration.ofSeconds(Long.parseLong(cmd.getOptionValue("rdelay", "3600"))))
					.withKafkaServer(cmd.getOptionValue("kafkaserver"))
					.withSeed(seed).build();

			producer.start();

		} catch (ParseException e) {
			// TODO [nku] parse
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
				Option.builder("speedup").hasArg().type(Integer.class).desc("stream speedup").build());

		options.addOption(
				Option.builder("rdelay").hasArg().type(Integer.class).desc("max random delay (sec)").build());

		options.addOption(Option.builder("sdelay").hasArg().type(Integer.class)
				.desc("max scheduling delay (sec)").build());

		options.addOption(Option.builder("seed").hasArg().type(Long.class).desc("random seed").build());

		options.addOption(Option.builder("worker").hasArg().type(Integer.class)
				.desc("number of threads executing scheduled tasks").build());

		return options;
	}
}
