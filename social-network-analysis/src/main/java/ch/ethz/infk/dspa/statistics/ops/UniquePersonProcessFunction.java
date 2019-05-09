package ch.ethz.infk.dspa.statistics.ops;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.google.common.collect.Streams;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;

public class UniquePersonProcessFunction extends KeyedProcessFunction<Long, PostActivity, Tuple3<Long, Long, Integer>> {

	private static final long serialVersionUID = 1L;

	private final long updateInterval;
	private final long windowSize;

	// <window timestamp, set<person id>>
	private MapState<Long, Set<Long>> state;

	public UniquePersonProcessFunction() {
		this(Time.hours(1), Time.hours(12));
	}

	public UniquePersonProcessFunction(Time updateInterval, Time windowSize) {
		this.updateInterval = updateInterval.toMilliseconds();
		this.windowSize = windowSize.toMilliseconds();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		state = getRuntimeContext().getMapState(new MapStateDescriptor<>(
				"UniquePersonProcessState",
				BasicTypeInfo.LONG_TYPE_INFO,
				TypeInformation.of(new TypeHint<Set<Long>>() {
				})));
	}

	@Override
	public void processElement(PostActivity value, Context ctx, Collector<Tuple3<Long, Long, Integer>> out)
			throws Exception {
		// compute the start and end of the current one hour window interval
		long intervalStart = TimeWindow.getWindowStartWithOffset(ctx.timestamp(), 0, this.updateInterval);
		long intervalEnd = intervalStart + this.updateInterval;

		// add the person id to the set for the current window
		Set<Long> updatedSet = state.contains(intervalStart) ? state.get(intervalStart) : new HashSet<>();
		updatedSet.add(value.getPersonId());
		state.put(intervalStart, updatedSet);

		// notify at the end of the current window interval
		ctx.timerService().registerEventTimeTimer(intervalEnd);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Long, Integer>> out)
			throws Exception {
		// compute the start of the 12-hour window
		long windowStart = timestamp - this.windowSize;

		// remove the window interval at the beginning of the 12-hour window
		// this interval has moved outside the window to be counted
		state.remove(windowStart - this.updateInterval);

		// leave early if the state does not contain any more keys
		// meaning that the post has gotten inactive and should not be maintained
		if (!state.keys().iterator().hasNext()) {
			state.clear();
			return;
		}

		// compute the global set of unique users over the last 12 hours
		Set<Long> globalSet = Streams.stream(state.entries())
				// filter events that lie beyond the scope of the window
				.filter(entry -> entry.getKey() < timestamp)
				.map(Map.Entry::getValue)
				// map all sets to streams and flatMap into a single stream
				.flatMap(Collection::stream)
				// collect the entire stream into a single set
				.collect(Collectors.toSet());

		// output a new unique people result tuple
		out.collect(Tuple3.of(ctx.getCurrentKey(), timestamp, globalSet.size()));

		// register a timer such that updates are printed every "updateInterval"
		// regardless of new events arriving
		ctx.timerService().registerEventTimeTimer(timestamp + this.updateInterval);
	}
}
