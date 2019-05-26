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

	private final double featureEnsembleThreshold;
	private final double fraudulentEventsThreshold;

	public EventStatisticsWindowProcessFunction(double featureEnsembleThreshold, double fraudulentEventsThreshold) {
		this.featureEnsembleThreshold = featureEnsembleThreshold;
		this.fraudulentEventsThreshold = fraudulentEventsThreshold;
	}

	@Override
	public void process(Long personId, Context context, Iterable<EventStatistics> elements,
			Collector<FraudulentUser> out) throws Exception {
		List<EventStatistics> anomalousEvents = Streams.stream(elements)
				.filter(eventStatistics -> eventStatistics.getIsAnomalousWithMajority(this.featureEnsembleThreshold))
				.collect(Collectors.toList());

		double fraudulentEventsRatio = ((double) anomalousEvents.size()) / Streams.stream(elements).count();
		if (fraudulentEventsRatio > this.fraudulentEventsThreshold) {
			FraudulentUser fraudulentUser = new FraudulentUser(personId);
			anomalousEvents.forEach(fraudulentUser::withVotesFrom);
			out.collect(fraudulentUser);
		}
	}

}
