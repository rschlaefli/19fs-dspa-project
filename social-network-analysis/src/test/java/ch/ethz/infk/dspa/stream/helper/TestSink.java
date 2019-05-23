package ch.ethz.infk.dspa.stream.helper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import ch.ethz.infk.dspa.ResultWindow;

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

	/**
	 * This function takes the sliding window which was originally used to create the results and bundles them together
	 * in a result window.
	 * 
	 * @param type
	 * @param slidingWindow which was originally used to create the results
	 * @return
	 */
	public static <T> List<ResultWindow<T>> getResultsInResultWindow(Class<T> type,
			SlidingEventTimeWindows slidingWindow) {
		Map<Long, List<T>> timestampedResultMap = getResultsTimestamped(type);

		// assign timestamped events to windows
		Map<TimeWindow, List<T>> timeWindowMap = new HashMap<>();

		TumblingEventTimeWindows assigner = TumblingEventTimeWindows.of(Time.milliseconds(slidingWindow.getSlide()));

		for (Entry<Long, List<T>> entry : timestampedResultMap.entrySet()) {
			Long timestamp = entry.getKey();
			List<T> results = entry.getValue();

			if (results.size() != 10) {
				System.out.println("here");
			}

			Collection<TimeWindow> timeWindows = assigner.assignWindows(null, timestamp, null);
			assert (timeWindows.size() == 1); // is assigned to tumbling window => can only assigned to one

			for (TimeWindow timeWindow : timeWindows) {
				if (!timeWindowMap.containsKey(timeWindow)) {
					timeWindowMap.put(timeWindow, new ArrayList<>());
				}
				timeWindowMap.get(timeWindow).addAll(results);
			}
		}

		// convert to result windows
		List<ResultWindow<T>> resultWindows = new ArrayList<>();
		for (Entry<TimeWindow, List<T>> entry : timeWindowMap.entrySet()) {
			TimeWindow timeWindow = entry.getKey();

			TimeWindow slidingTimeWindow = new TimeWindow(timeWindow.getEnd() - slidingWindow.getSize(),
					timeWindow.getEnd());
			List<T> results = entry.getValue();

			if (results.size() != 10) {
				System.out.println("here");
			}

			ResultWindow<T> resultWindow = ResultWindow.of(slidingTimeWindow);
			resultWindow.setResults(results);
			resultWindows.add(resultWindow);
		}

		// sort result windows
		resultWindows = resultWindows.stream()
				.sorted(Comparator.comparing(ResultWindow::getStart))
				.collect(Collectors.toList());

		return resultWindows;

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
