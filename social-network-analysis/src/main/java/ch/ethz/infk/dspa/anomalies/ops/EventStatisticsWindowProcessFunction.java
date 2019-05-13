package ch.ethz.infk.dspa.anomalies.ops;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.google.common.collect.Streams;

import ch.ethz.infk.dspa.anomalies.dto.EventStatistics;
import ch.ethz.infk.dspa.anomalies.dto.FraudulentUser;

public class EventStatisticsWindowProcessFunction
		extends ProcessWindowFunction<EventStatistics, FraudulentUser, Long, TimeWindow> {

	private static final long serialVersionUID = 1L;

	private double isFraudulentThreshold;

	public EventStatisticsWindowProcessFunction(double isFraudulentThreshold) {
		this.isFraudulentThreshold = isFraudulentThreshold;
	}

	@Override
	public void process(Long personId, Context context, Iterable<EventStatistics> elements,
			Collector<FraudulentUser> out) throws Exception {
		List<EventStatistics> anomalousEvents = Streams.stream(elements)
				.filter(eventStatistics -> eventStatistics.getIsAnomalousWithMajority(this.isFraudulentThreshold))
				.collect(Collectors.toList());

		// TODO [rsc]: change to incremental implementation?
		if (anomalousEvents.size() / Streams.stream(elements).count() > this.isFraudulentThreshold) {
			FraudulentUser fraudulentUser = new FraudulentUser(personId);
			anomalousEvents.forEach(fraudulentUser::withVotesFrom);
			out.collect(fraudulentUser);
		}
	}

}
