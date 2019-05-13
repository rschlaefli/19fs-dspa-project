package ch.ethz.infk.dspa;

import ch.ethz.infk.dspa.stream.connectors.KafkaProducerBuilder;
import ch.ethz.infk.dspa.helper.Config;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.commons.configuration2.Configuration;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.anomalies.AnomaliesAnalyticsTask;
import ch.ethz.infk.dspa.recommendations.RecommendationsAnalyticsTask;
import ch.ethz.infk.dspa.statistics.ActivePostsAnalyticsTask;

public class App {
	public static void main(String[] args) throws Exception {
		Configuration config = Config.getConfig();
		final String kafkaServer = config.getString("kafka.server");
		final String staticFilePath = config.getString("files.staticPath");

		try {
			Options options = buildOptions();

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			// parse command line arguments
			final String analyticsType = cmd.getOptionValue("analyticstype");
			final long maxDelaySeconds = Long
					.parseLong(cmd.getOptionValue("maxdelaysec", config.getString("defaults.maxDelayInSeconds")));

			// TODO [nku]: make use of the seed
			// Long seed = cmd.getOptionValue("seed") != null ? Long.parseLong(cmd.getOptionValue("seed")) :
			// null;

			AbstractAnalyticsTask<?, ?> analyticsTask;
			String outputTopic;

			switch (analyticsType) {
			case "activeposts":
				analyticsTask = new ActivePostsAnalyticsTask();
				outputTopic = "active-posts-out";
				break;
			case "recommendations":
				analyticsTask = new RecommendationsAnalyticsTask();
				outputTopic = "recommendations-out";
				break;
			case "anomalies":
				analyticsTask = new AnomaliesAnalyticsTask();
				outputTopic = "anomalies-out";
				break;
			default:
				throw new IllegalArgumentException("INVALID_ANALYTICS_TYPE");
			}

			// TODO: add any necessary exec. config for producers
			ExecutionConfig executionConfig = new ExecutionConfig();

			SinkFunction<String> kafkaSink = new KafkaProducerBuilder<String>()
					.withClass(String.class)
					.withKafkaConnection(kafkaServer)
					.withTopic(outputTopic)
					.withExecutionConfig(executionConfig)
					.build();

			analyticsTask
					.withKafkaServer(kafkaServer)
					.withStaticFilePath(staticFilePath)
					.withMaxDelay(Time.seconds(maxDelaySeconds))
					.initialize()
					.build()
					.toStringStream()
					.addSink(kafkaSink);

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

		options.addOption(
				Option.builder("analyticstype")
						.hasArg()
						.required()
						.type(String.class)
						.desc("analytics type")
						.build());

		options.addOption(
				Option.builder("maxdelaysec")
						.hasArg()
						.type(Long.class)
						.desc("maximum delay in seconds")
						.build());

		return options;
	}
}
