package ch.ethz.infk.dspa.statistics.ops;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;

public class TypeCountAggregateFunction
		implements AggregateFunction<PostActivity, Long, Long> {

	private static final long serialVersionUID = 1L;
	private final ActivityType TYPE;

	public TypeCountAggregateFunction(ActivityType type) {
		this.TYPE = type;
	}

	@Override
	public Long createAccumulator() {
		return 0l;
	}

	@Override
	public Long add(PostActivity activity, Long accumulator) {

		if (TYPE == activity.getType()) {
			return accumulator += 1;
		}

		return accumulator;
	}

	@Override
	public Long getResult(Long accumulator) {
		return accumulator;
	}

	@Override
	public Long merge(Long a, Long b) {
		return a + b;
	}

}
