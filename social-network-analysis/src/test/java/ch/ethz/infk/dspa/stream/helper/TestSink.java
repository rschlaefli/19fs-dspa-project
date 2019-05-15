package ch.ethz.infk.dspa.stream.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TestSink<IN> implements SinkFunction<IN> {

	private static final long serialVersionUID = 1L;

	public static Map<Long, List<Object>> sink = Collections.synchronizedMap(new HashMap<Long, List<Object>>());

	public TestSink() {
		reset();
	}

	@Override
	public void invoke(IN value, @SuppressWarnings("rawtypes") Context ctx) throws Exception {
		SinkFunction.super.invoke(value, ctx);
		long timestamp = ctx.timestamp();
		sink.putIfAbsent(timestamp, new ArrayList<Object>());
		sink.get(timestamp).add(value);
	}

	public static void reset() {
		sink.clear();
	}

	public static <T> Map<Long, List<T>> getResultsInTumblingWindow(Class<T> type, Time windowSize) {
		Map<Long, List<T>> timestampedResultMap = getResultsTimestamped(type);
		Map<Long, List<T>> windowedResultMap = new HashMap<>();

		// move all timestamped results in their respective tumbling window
		for (Entry<Long, List<T>> entry : timestampedResultMap.entrySet()) {
			Long timestamp = entry.getKey();
			List<T> results = entry.getValue();
			Long windowStart = TimeWindow.getWindowStartWithOffset(timestamp, 0, windowSize.toMilliseconds());

			windowedResultMap.putIfAbsent(windowStart, new ArrayList<T>()); // init if not present
			windowedResultMap.get(windowStart).addAll(results);
		}

		return windowedResultMap;

	}

	public static <T> Map<Long, List<T>> getResultsTimestamped(Class<T> type) {
		Map<Long, List<T>> resultMap = new HashMap<>();

		for (Entry<Long, List<Object>> entry : sink.entrySet()) {
			Long timestamp = entry.getKey();
			List<T> results = entry.getValue().stream().map(x -> type.cast(x)).collect(Collectors.toList());
			resultMap.put(timestamp, results);
		}
		return resultMap;
	}

	public static <T> List<T> getResults(Class<T> type) {
		return sink.entrySet().stream()
				.flatMap(entry -> entry.getValue().stream())
				.map(x -> type.cast(x))
				.collect(Collectors.toList());

	}

}
