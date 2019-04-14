package ch.ethz.infk.dspa.recommendations.ops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;

public class PersonActivityBroadcastJoinProcessFunction
		extends BroadcastProcessFunction<PersonActivity, PersonActivity, PersonSimilarity>
		implements CheckpointedFunction {

	public static final MapStateDescriptor<Long, List<PersonActivity>> SELECTED_PERSON_STATE_DESCRIPTOR = new MapStateDescriptor<>(
			"SelectedPersonActivityBroadcastState",
			BasicTypeInfo.LONG_TYPE_INFO,
			TypeInformation.of(new TypeHint<List<PersonActivity>>() {
			}));

	private static final long serialVersionUID = 1L;

	private int broadcastElementCount;
	private long windowSize;
	private long windowOffset;

	private HashMap<Long, List<PersonActivity>> buffer;
	private ListState<Entry<Long, List<PersonActivity>>> checkpointedBufferState;

	public PersonActivityBroadcastJoinProcessFunction(int broadcastElementCount, Time windowSize) {
		super();

		this.broadcastElementCount = broadcastElementCount;
		this.windowSize = windowSize.toMilliseconds();
		this.windowOffset = 0;

	}

	// TODO [nku] possible optimization, output similarity only if not already
	// output 10 higher similarities for p1

	public PersonSimilarity getSimilarity(PersonActivity activity1, PersonActivity activity2) {

		HashMap<String, Integer> firstMap = activity1.categoryMap();
		HashMap<String, Integer> secondMap = activity2.categoryMap();

		Set<String> keys = new HashSet<>();
		keys.addAll(firstMap.keySet());
		keys.addAll(secondMap.keySet());

		double sum = 0;

		for (String key : keys) {
			sum += firstMap.getOrDefault(key, 0) * secondMap.getOrDefault(key, 0);
		}

		return new PersonSimilarity()
				.withPerson1Id(activity1.personId())
				.withPerson2Id(activity2.personId())
				.withSimilarity(sum); // TODO [nku] not sure if we should normalize: .withSimilarity(sum /
										// keys.size())
	}

	@Override
	public void processBroadcastElement(PersonActivity activity, Context ctx, Collector<PersonSimilarity> out)
			throws Exception {

		long windowStart = TimeWindow.getWindowStartWithOffset(ctx.timestamp(), this.windowOffset, this.windowSize);

		BroadcastState<Long, List<PersonActivity>> state = ctx.getBroadcastState(SELECTED_PERSON_STATE_DESCRIPTOR);

		List<PersonActivity> list = state.get(windowStart);

		if (list == null) {
			list = new ArrayList<>(broadcastElementCount);
		}

		// TODO [nku] check if more than broadcastElementCount elements arrived for same
		// window, if yes -> throw exception
		list.add(activity);
		boolean isComplete = list.size() == broadcastElementCount;
		state.put(windowStart, list);

		// join activity with
		for (PersonActivity other : buffer.getOrDefault(windowStart, new ArrayList<>())) {
			PersonSimilarity similarity = getSimilarity(activity, other);
			out.collect(similarity);
		}

		// clear the buffer if all broadcast elements arrived for this window
		if (isComplete) {
			buffer.remove(windowStart);
		}

		// clean up expired windows
		long lastActiveWindowStart = TimeWindow.getWindowStartWithOffset(ctx.currentWatermark(), this.windowOffset,
				this.windowSize);

		Set<Long> expiredKeys = new HashSet<>();
		for (Entry<Long, List<PersonActivity>> entry : state.entries()) {
			if (entry.getKey() < lastActiveWindowStart) {
				expiredKeys.add(entry.getKey());
			}
		}

		for (Long key : expiredKeys) {
			// TODO [nku] check if buffer still has elements, if yes, throw exception
			state.remove(key);
		}

	}

	@Override
	public void processElement(PersonActivity otherActivity, ReadOnlyContext roCtx, Collector<PersonSimilarity> out)
			throws Exception {

		long windowStart = TimeWindow.getWindowStartWithOffset(roCtx.timestamp(), this.windowOffset, this.windowSize);

		ReadOnlyBroadcastState<Long, List<PersonActivity>> state = roCtx
				.getBroadcastState(SELECTED_PERSON_STATE_DESCRIPTOR);

		List<PersonActivity> broadcastActivities = state.get(windowStart);

		if (broadcastActivities == null) {
			broadcastActivities = new ArrayList<>();
		}

		for (PersonActivity activity : broadcastActivities) {
			PersonSimilarity similarity = getSimilarity(activity, otherActivity);
			out.collect(similarity);
		}

		boolean isComplete = broadcastActivities.size() == broadcastElementCount;

		// if not all broadcast elements have arrived yet, need to buffer the element
		// such that it can be joined later
		if (!isComplete) {
			List<PersonActivity> activities = buffer.getOrDefault(windowStart, new ArrayList<PersonActivity>());
			activities.add(otherActivity);
			buffer.put(windowStart, activities);
		}

	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		checkpointedBufferState.clear();

		for (Entry<Long, List<PersonActivity>> e : buffer.entrySet()) {
			checkpointedBufferState.add(e);
		}

	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		ListStateDescriptor<Entry<Long, List<PersonActivity>>> descriptor = new ListStateDescriptor<>(
				"BufferedActivities",
				TypeInformation.of(new TypeHint<Entry<Long, List<PersonActivity>>>() {
				}));

		checkpointedBufferState = context.getOperatorStateStore().getListState(descriptor);

		buffer = new HashMap<>();

		if (context.isRestored()) {
			for (Entry<Long, List<PersonActivity>> e : checkpointedBufferState.get()) {
				buffer.put(e.getKey(), e.getValue());
			}
		}

	}

}
