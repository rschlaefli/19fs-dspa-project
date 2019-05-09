package ch.ethz.infk.dspa.anomalies.ops;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.anomalies.dto.EventStatistics;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;

public class EnsembleAggregationFunction
		implements AggregateFunction<FeatureStatistics, EventStatistics, EventStatistics> {

	// TODO: maybe we can reduce this or get rid of this function altogether?

	private static final long serialVersionUID = 1L;

	@Override
	public EventStatistics createAccumulator() {
		return new EventStatistics();
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
