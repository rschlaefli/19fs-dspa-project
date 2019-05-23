package ch.ethz.infk.dspa;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.Configuration;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.anomalies.AnomaliesAnalyticsTask;
import ch.ethz.infk.dspa.helper.Config;
import ch.ethz.infk.dspa.recommendations.RecommendationsAnalyticsTask;
import ch.ethz.infk.dspa.statistics.ActivePostsAnalyticsTask;
import ch.ethz.infk.dspa.stream.connectors.KafkaProducerBuilder;

public class App {
	public static void main(String[] args) throws Exception {
		try {
			Options options = buildOptions();

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			// parse command line arguments
			final String analyticsType = cmd.getOptionValue("analyticstype");
			final String configPath = cmd.getOptionValue("configpath");
			final Boolean withWebUi = cmd.hasOption("webui");

			// read file based configuration
			final Configuration config = Config.getConfig(configPath);

			// read command line arguments with file based defaults
			final long maxDelaySeconds = Long
					.parseLong(cmd.getOptionValue("maxdelaysec", config.getString("stream.maxDelayInSeconds")));
			final String kafkaServer = cmd.getOptionValue("kafkaserver", config.getString("kafka.server"));
			final String staticFilePath = cmd.getOptionValue("staticpath", config.getString("files.staticPath"));

			AbstractAnalyticsTask<?, ?> analyticsTask;
			String outputTopic;

			switch (analyticsType) {
			case "activeposts":
				analyticsTask = new ActivePostsAnalyticsTask();
				outputTopic = "active-posts-out";
				break;
			case "recommendations":
				String[] personIdArgs = cmd.getOptionValues("personIds");
				if (personIdArgs != null) {
					// convert to longs
					final Set<Long> recommendationPersonIds = Arrays.asList(personIdArgs).stream()
							.map(pId -> Long.parseLong(pId)).collect(Collectors.toSet());
					// set personIds in RecommendationsAnalyticsTask
					analyticsTask = new RecommendationsAnalyticsTask()
							.withRecommendationPersonIds(recommendationPersonIds);
				} else {
					analyticsTask = new RecommendationsAnalyticsTask();
				}

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

			StreamExecutionEnvironment env;
			if (withWebUi) {
				org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
				conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
				conf.setInteger(RestOptions.PORT, 8082);
				env = StreamExecutionEnvironment.createLocalEnvironment(Runtime.getRuntime().availableProcessors(),
						conf);
			} else {
				env = StreamExecutionEnvironment.getExecutionEnvironment();
			}

			analyticsTask
					.withStreamingEnvironment(env)
					.withPropertiesConfiguration(config)
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
				Option.builder("kafkaserver")
						.hasArg()
						.type(String.class)
						.desc("kafka server")
						.build());

		options.addOption(
				Option.builder("maxdelaysec")
						.hasArg()
						.type(Long.class)
						.desc("maximum delay in seconds")
						.build());
		options.addOption(
				Option.builder("configpath")
						.hasArg()
						.type(String.class)
						.desc("config file path")
						.build());

		options.addOption(
				Option.builder("staticpath")
						.hasArg()
						.type(String.class)
						.desc("static file path")
						.build());

		options.addOption(
				Option.builder("webui")
						.desc("start local web ui")
						.build());

		options.addOption(
				Option.builder("personIds")
						.hasArgs()
						.type(Long.class)
						.desc("for recommendations: personIds for which to generate recommendations").build());

		return options;
	}
}
