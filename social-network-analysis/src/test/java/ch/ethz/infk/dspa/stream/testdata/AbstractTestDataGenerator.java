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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

import scala.NotImplementedError;

public abstract class AbstractTestDataGenerator<T> {

	public abstract TestDataPair<T> parseLine(String line);

	public abstract DataStream<T> addReturnType(SingleOutputStreamOperator<T> out);

	public AssignerWithPeriodicWatermarks<TestDataPair<T>> getTimestampsAndWatermarkAssigner(
			Time maxOutOfOrderness) {
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
	 * 
	 * @throws IOException
	 */
	public List<T> generate(String file) throws IOException {
		return generateTestData(file).stream().map(x -> x.element).collect(Collectors.toList());
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
		List<TestDataPair<T>> data = generateTestData(file);
		DataStream<TestDataPair<T>> stream = env.fromCollection(data);
		SingleOutputStreamOperator<T> out = stream
				.assignTimestampsAndWatermarks(getTimestampsAndWatermarkAssigner(maxOutOfOrderness))
				.map(x -> {
					// TODO Uncomment for debugging, this allows that watermarks can be better
					// observed with the DebugProcessFunction if no window was applied
					// TimeUnit.MILLISECONDS.sleep(100);
					return x.element;
				});
		return addReturnType(out);
	}

	private List<TestDataPair<T>> generateTestData(String file) throws IOException {

		List<TestDataPair<T>> stream = new ArrayList<>();

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {

			br.readLine(); // skip header
			String line;

			while ((line = br.readLine()) != null && StringUtils.isNotEmpty(line)) {
				stream.add(parseLine(line));

			}
		}

		return stream;

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

	public static class TestDataPair<T> {
		T element;
		DateTime timestamp;

		public static <T> TestDataPair<T> of(T element, DateTime timestamp) {
			TestDataPair<T> c = new TestDataPair<>();
			c.element = element;
			c.timestamp = timestamp;
			return c;
		}
	}

}
