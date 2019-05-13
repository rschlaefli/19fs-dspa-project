package ch.ethz.infk.dspa.statistics.ops;

import ch.ethz.infk.dspa.statistics.dto.StatisticsOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class WindowInfoProcessFunction extends ProcessFunction<StatisticsOutput, StatisticsOutput> {

	private static final long serialVersionUID = 1L;

	@Override
	public void processElement(StatisticsOutput element, Context ctx, Collector<StatisticsOutput> out)
			throws Exception {
		element.setTimestamp(ctx.timestamp());
		out.collect(element);
	}

}
