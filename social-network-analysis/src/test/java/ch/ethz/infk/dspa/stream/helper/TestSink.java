package ch.ethz.infk.dspa.stream.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class TestSink<IN> implements SinkFunction<IN> {

	private static final long serialVersionUID = 1L;

	public static List<Object> sink = Collections.synchronizedList(new ArrayList<>());

	public TestSink() {
		reset();
	}

	public static void reset() {
		sink.clear();
	}

	public static <T> List<T> getResults(Class<T> type) {
		Stream<T> stream = sink.stream().map(x -> type.cast(x));
		return stream.collect(Collectors.toList());
	}

	@Override
	public void invoke(IN value) throws Exception {
		sink.add(value);
	}

}