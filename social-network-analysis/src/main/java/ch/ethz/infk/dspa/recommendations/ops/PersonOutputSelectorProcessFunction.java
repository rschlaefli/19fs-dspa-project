package ch.ethz.infk.dspa.recommendations.ops;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class PersonOutputSelectorProcessFunction extends ProcessFunction<PersonActivity, PersonActivity>
		implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;
	public static OutputTag<PersonActivity> SELECTED = new OutputTag<>("selected",
			TypeInformation.of(PersonActivity.class));
	private final int selectionCount;
	private final long windowSize;

	private transient ListState<Map.Entry<Long, Integer>> checkpointedState;
	private Map<Long, Integer> selectedActivities;

	public PersonOutputSelectorProcessFunction(int selectionCount, Time windowSize) {
		this.selectionCount = selectionCount;
		this.windowSize = windowSize.toMilliseconds();
		this.selectedActivities = new HashMap<>();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		ListStateDescriptor<Map.Entry<Long, Integer>> descriptor = new ListStateDescriptor<>(
				"ActivitySelectionCounts",
				TypeInformation.of(new TypeHint<Map.Entry<Long, Integer>>() {
				}));
		this.checkpointedState = context.getOperatorStateStore().getListState(descriptor);

		if (context.isRestored()) {
			for (Map.Entry<Long, Integer> e : this.checkpointedState.get()) {
				this.selectedActivities.put(e.getKey(), e.getValue());
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		this.checkpointedState.clear();
		for (Map.Entry<Long, Integer> e : this.selectedActivities.entrySet()) {
			this.checkpointedState.add(e);
		}
	}

	@Override
	public void processElement(PersonActivity activity, Context ctx, Collector<PersonActivity> out) {

		long windowStart = TimeWindow.getWindowStartWithOffset(ctx.timestamp(), 0, this.windowSize);

		// check the count of selected activities for the current window
		// and output a new selection if required
		int currentCount = this.selectedActivities.getOrDefault(windowStart, 0);
		if (currentCount < this.selectionCount) {
			this.selectedActivities.put(windowStart, currentCount + 1);
			ctx.output(SELECTED, activity);
		}

		// remove keys that are outdated compared to the current watermark
		this.selectedActivities.keySet().removeIf(key -> key + this.windowSize < ctx.timerService().currentWatermark());

		out.collect(activity);
	}
}
