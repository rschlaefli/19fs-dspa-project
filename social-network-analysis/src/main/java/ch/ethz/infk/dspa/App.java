package ch.ethz.infk.dspa;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.anomalies.AnomaliesAnalyticsTask;
import ch.ethz.infk.dspa.recommendations.RecommendationsAnalyticsTask;
import ch.ethz.infk.dspa.statistics.ActivePostsAnalyticsTask;

public class App {
	public static void main(String[] args) throws Exception {
		Options options = buildOptions();

		try {

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			// parse command line arguments
			String analyticsType = cmd.getOptionValue("analyticstype");
			String kafkaServer = cmd.getOptionValue("kafkaserver");
			long maxDelaySeconds = Long.parseLong(cmd.getOptionValue("maxdelaysec"));

			// TODO [nku]: make use of the seed
			// Long seed = cmd.getOptionValue("seed") != null ? Long.parseLong(cmd.getOptionValue("seed")) :
			// null;

			AbstractAnalyticsTask<?, ?> analyticsTask;

			switch (analyticsType) {
			case "activeposts":
				analyticsTask = new ActivePostsAnalyticsTask();
				break;
			case "recommendations":
				analyticsTask = new RecommendationsAnalyticsTask();
				break;
			case "anomalies":
				analyticsTask = new AnomaliesAnalyticsTask();
				break;
			default:
				throw new IllegalArgumentException("INVALID_ANALYTICS_TYPE");
			}

			analyticsTask
					.withKafkaServer(kafkaServer)
					.withStaticFilePath("./../data/1k-users-sorted/tables/")
					.withMaxDelay(Time.seconds(maxDelaySeconds))
					.initialize()
					.build();

			try {
				analyticsTask.start();
			} catch (Exception e) {
				e.printStackTrace();
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

		options.addOption(Option.builder("analyticstype").hasArg().required().type(String.class)
				.desc("analytics type").build());

		options.addOption(Option.builder("maxdelaysec").hasArg().required().type(Long.class)
				.desc("maximum delay in seconds").build());

		/*
		 * options.addOption(Option.builder("consumergroup").hasArg().required().type(String.class)
		 * .desc("kafka consumer group").build());
		 */

		return options;
	}
}
