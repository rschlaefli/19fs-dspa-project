package ch.ethz.infk.dspa.anomalies.ops;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.anomalies.dto.EventStatistics;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;

import java.util.Map;

public class EnsembleAggregationFunction
		implements AggregateFunction<FeatureStatistics, EventStatistics, EventStatistics> {

	// TODO: maybe we can reduce this or get rid of this function altogether?

	private static final long serialVersionUID = 1L;

	private final ImmutableMap<Feature.FeatureId, Double> thresholds;

	public EnsembleAggregationFunction(ImmutableMap<Feature.FeatureId, Double> thresholds) {
		this.thresholds = thresholds;
	}

	@Override
	public EventStatistics createAccumulator() {
		return new EventStatistics(this.thresholds);
	}

	@Override
	public EventStatistics add(FeatureStatistics value, EventStatistics accumulator) {
		return accumulator.addFeatureVote(value);
	}

	@Override
	public EventStatistics getResult(EventStatistics accumulator) {
		return accumulator;
	}

	@Override
	public EventStatistics merge(EventStatistics a, EventStatistics b) {
		return a.withVotesFrom(b);
	}

}
