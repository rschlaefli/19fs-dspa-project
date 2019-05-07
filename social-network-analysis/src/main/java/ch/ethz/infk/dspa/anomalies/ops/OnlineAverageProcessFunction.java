package ch.ethz.infk.dspa.anomalies.ops;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class OnlineAverageProcessFunction extends KeyedProcessFunction<Feature.FeatureId, Feature, FeatureStatistics> {

    // TODO: implementation

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(Feature feature, Context ctx, Collector<FeatureStatistics> out) throws Exception {
        // TODO: incrementally build the statistics (std and mean)

        List<Feature> list = new ArrayList<>();
        list.add(feature);
        out.collect(new FeatureStatistics(feature).withStatisticsFrom(list));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FeatureStatistics> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }

}
