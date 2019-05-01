package ch.ethz.infk.dspa.statistics.ops;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class WindowInfoProcessFunction<T> extends ProcessFunction<T, Tuple2<Long, T>> {

	private static final long serialVersionUID = 1L;

	@Override
	public void processElement(T element, Context ctx, Collector<Tuple2<Long, T>> out) throws Exception {
		Long timestamp = ctx.timestamp();
		out.collect(Tuple2.of(timestamp, element));
	}

}
