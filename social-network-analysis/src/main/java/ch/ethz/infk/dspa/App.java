package ch.ethz.infk.dspa;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import ch.ethz.infk.dspa.statistics.ActivePostsStatistics;

public class App {
	public static void main(String[] args) {
		Options options = buildOptions();

		try {

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			// TODO: make use of the seed
			Long seed = cmd.getOptionValue("seed") != null ? Long.parseLong(cmd.getOptionValue("seed")) : null;

			String consumerType = cmd.getOptionValue("consumertype");

			if (consumerType.equals("activeposts")) {

				ActivePostsStatistics consumer = new ActivePostsStatistics()
					.withKafkaServer(cmd.getOptionValue("kafkaserver"));

				consumer.start();

			} else if (consumerType.equals("recommendations")) {
				// TODO
			} else if (consumerType.equals("anomalies")) {
				// TODO
			} else {
				throw new IllegalArgumentException("INVALID_CONSUMER_TYPE");
			}

		} catch (ParseException e) {
			// TODO [nku] parse
			e.printStackTrace();
		}
	}

	private static Options buildOptions() {
		Options options = new Options();

		options.addOption(Option.builder("kafkaserver").hasArg().required().type(String.class)
				.desc("kafka server").build());

		options.addOption(Option.builder("consumertype").hasArg().required().type(String.class)
				.desc("consumer type").build());

		/* options.addOption(Option.builder("consumergroup").hasArg().required().type(String.class)
				.desc("kafka consumer group").build()); */

		return options;
	}
}
