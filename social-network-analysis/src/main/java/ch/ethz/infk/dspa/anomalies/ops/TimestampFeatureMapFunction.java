package ch.ethz.infk.dspa.anomalies.ops;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class TimestampFeatureMapFunction extends RichMapFunction<Feature, Feature> {

    @Override
    public Feature map(Feature value) throws Exception {
        return value;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
}
