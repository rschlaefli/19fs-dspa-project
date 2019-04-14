package ch.ethz.infk.dspa.stream.testdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.time.Time;

import scala.NotImplementedError;

public abstract class TestDataGenerator<T> {

	public abstract T parseLine(String line);

	public AssignerWithPeriodicWatermarks<T> getTimestampsAndWatermarkAssigner(Time maxOutOfOrderness) {
		throw new NotImplementedError("Not implemented");
	}

	/**
	 * Generates a single Dummy Element
	 */
	public T generateElement() {
		throw new NotImplementedError("Not implemented");
	}

	/**
	 * Generate List of T from file
	 */
	public List<T> generate(String file) throws IOException {

		List<T> stream = new ArrayList<>();

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {

			br.readLine(); // skip header
			String line;

			while ((line = br.readLine()) != null && StringUtils.isNotEmpty(line)) {
				stream.add(parseLine(line));

			}
		}

		return stream;

	}

	/**
	 * Generate DataStream without timestamps and watermarks
	 */
	public DataStream<T> generate(StreamExecutionEnvironment env, String file)
			throws IOException {
		List<T> data = generate(file);
		DataStream<T> stream = env.fromCollection(data);

		return stream;
	}

	/**
	 * Generate DataStream with timestamps and watermarks
	 */
	public DataStream<T> generate(StreamExecutionEnvironment env, String file, Time maxOutOfOrderness)
			throws IOException {
		DataStream<T> stream = generate(env, file);
		return stream.assignTimestampsAndWatermarks(getTimestampsAndWatermarkAssigner(maxOutOfOrderness));
	}

	protected List<Long> parseLongList(String str) {

		if (StringUtils.isEmpty(str)) {
			return null;
		}

		return Stream.of(
				str.replaceAll("\\s+", "")
						.replaceAll("\\[", "")
						.replaceAll("\\]", "")
						.split(","))
				.map(x -> Long.parseLong(x))
				.collect(Collectors.toList());

	}

}
