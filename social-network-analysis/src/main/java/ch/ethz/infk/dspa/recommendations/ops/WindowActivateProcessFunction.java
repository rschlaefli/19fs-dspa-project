package ch.ethz.infk.dspa.recommendations.ops;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class WindowActivateProcessFunction extends ProcessFunction<PersonActivity, Tuple0>
		implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;

	private long windowSize;
	private Set<Long> windows;
	private ListState<Long> checkpointedWindows;

	public WindowActivateProcessFunction(Time windowSize) {
		this.windowSize = windowSize.toMilliseconds();
		this.windows = new HashSet<>();
	}

	@Override
	public void processElement(PersonActivity in, Context ctx, Collector<Tuple0> out) throws Exception {
		long window = TimeWindow.getWindowStartWithOffset(ctx.timestamp(), 0, windowSize);

		if (!windows.contains(window)) {
			windows.add(window);
			out.collect(new Tuple0());
		}

		// remove expired windows (for which no more new events can arrive)
		windows.removeIf(x -> x + windowSize < ctx.timerService().currentWatermark());
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		checkpointedWindows.clear();
		checkpointedWindows.addAll(windows.stream().collect(Collectors.toList()));
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
				"recommendations-window-triggers",
				Long.class);

		checkpointedWindows = context.getOperatorStateStore().getListState(descriptor);
		windows = new HashSet<>();

		if (context.isRestored()) {
			for (Long window : checkpointedWindows.get()) {
				windows.add(window);
			}
		}
	}
}
