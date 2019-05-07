package ch.ethz.infk.dspa.anomalies.ops;

import ch.ethz.infk.dspa.anomalies.dto.EventStatistics;
import ch.ethz.infk.dspa.anomalies.dto.FraudulentUser;
import com.google.common.collect.Streams;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class EventStatisticsWindowProcessFunction extends ProcessWindowFunction<EventStatistics, FraudulentUser, Long, TimeWindow> {

    private double isFraudulentThreshold;

    public EventStatisticsWindowProcessFunction() {
        this(0.75);
    }

    public EventStatisticsWindowProcessFunction(double isFraudulentThreshold) {
        this.isFraudulentThreshold = isFraudulentThreshold;
    }

    @Override
    public void process(Long personId, Context context, Iterable<EventStatistics> elements, Collector<FraudulentUser> out) throws Exception {
        Stream<EventStatistics> anomalousEvents = Streams.stream(elements)
                .filter(eventStatistics -> eventStatistics.getIsAnomalousWithMajority(0.8));

        // TODO [rsc]: change to incremental implementation?
        if (anomalousEvents.count() / Streams.stream(elements).count() > this.isFraudulentThreshold) {
            FraudulentUser fraudulentUser = new FraudulentUser(personId);
            anomalousEvents.forEach(fraudulentUser::withVotesFrom);
            out.collect(fraudulentUser);
        }
    }

}
