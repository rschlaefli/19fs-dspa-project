package ch.ethz.infk.dspa.debug;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DebugProcessFunction<T> extends ProcessFunction<T, T> {

	private static final long serialVersionUID = 1L;
	private boolean printElement;

	public DebugProcessFunction() {
		this.printElement = false;
	}

	public DebugProcessFunction(boolean printElement) {
		this.printElement = printElement;
	}

	@Override
	public void processElement(T element, ProcessFunction<T, T>.Context ctx, Collector<T> out) throws Exception {

		Long timestamp = ctx.timestamp();
		Long watermark = ctx.timerService().currentWatermark();

		String info = String.format("Timestamp: %d   Watermark: %d", timestamp, watermark);
		System.out.println(info);

		if (this.printElement) {
			System.out.println(element);
		}

		out.collect(element);
	}

}
