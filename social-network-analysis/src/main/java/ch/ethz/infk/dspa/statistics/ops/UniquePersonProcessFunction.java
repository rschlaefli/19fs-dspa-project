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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.google.common.collect.Streams;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.StatisticsOutput;

public class UniquePersonProcessFunction extends KeyedProcessFunction<Long, PostActivity, StatisticsOutput> {

	private static final long serialVersionUID = 1L;

	private final long updateInterval;
	private final long windowSize;

	// <window timestamp, set<person id>>
	private MapState<Long, Set<Long>> state;

	public UniquePersonProcessFunction(Time updateInterval, Time windowSize) {
		this.updateInterval = updateInterval.toMilliseconds();
		this.windowSize = windowSize.toMilliseconds();
	}

	@Override
	public void open(Configuration parameters) {
		this.state = getRuntimeContext().getMapState(new MapStateDescriptor<>(
				"UniquePersonProcessState",
				BasicTypeInfo.LONG_TYPE_INFO,
				TypeInformation.of(new TypeHint<Set<Long>>() {
				})));
	}

	@Override
	public void processElement(PostActivity value, Context ctx, Collector<StatisticsOutput> out)
			throws Exception {
		// compute the start and end of the current one hour window interval
		long intervalStart = TimeWindow.getWindowStartWithOffset(ctx.timestamp(), 0, this.updateInterval);
		long intervalEnd = intervalStart + this.updateInterval - 1;

		// add the person id to the set for the current window
		Set<Long> updatedSet = this.state.contains(intervalStart) ? this.state.get(intervalStart) : new HashSet<>();
		updatedSet.add(value.getPersonId());
		this.state.put(intervalStart, updatedSet);

		// notify at the end of the current window interval
		ctx.timerService().registerEventTimeTimer(intervalEnd);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<StatisticsOutput> out)
			throws Exception {
		// compute the start of the 12-hour window
		long windowStart = timestamp + 1 - this.windowSize;

		// remove the window interval at the beginning of the 12-hour window
		// this interval has moved outside the window to be counted
		this.state.remove(windowStart - this.updateInterval);

		// compute the global set of unique users over the last 12 hours
		Set<Long> globalSet = Streams.stream(this.state.entries())
				// filter events that lie beyond the scope of the window
				.filter(entry -> entry.getKey() < timestamp)
				.map(Map.Entry::getValue)
				// map all sets to streams and flatMap into a single stream
				.flatMap(Collection::stream)
				// collect the entire stream into a single set
				.collect(Collectors.toSet());

		// if the global set has size 0 => there has not been an interaction with the post within the last 12 hours
		// => is inactive (
		if (globalSet.size() > 0) {

			// output a new unique people result tuple
			out.collect(new StatisticsOutput(ctx.getCurrentKey(), (long) globalSet.size(),
					StatisticsOutput.OutputType.UNIQUE_PERSON_COUNT));

			// register a timer such that updates are printed every "updateInterval"
			// regardless of new events arriving
			ctx.timerService().registerEventTimeTimer(timestamp + this.updateInterval);

		}
	}
}
