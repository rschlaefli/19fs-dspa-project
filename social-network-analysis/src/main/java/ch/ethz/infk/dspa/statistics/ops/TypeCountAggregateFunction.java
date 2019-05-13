package ch.ethz.infk.dspa.statistics.ops;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;
import ch.ethz.infk.dspa.statistics.dto.StatisticsOutput;

public class TypeCountAggregateFunction
		implements AggregateFunction<PostActivity, StatisticsOutput, StatisticsOutput> {

	private static final long serialVersionUID = 1L;
	private final ActivityType TYPE;

	public TypeCountAggregateFunction(ActivityType type) {
		this.TYPE = type;
	}

	@Override
	public StatisticsOutput createAccumulator() {
		StatisticsOutput accumulator = new StatisticsOutput();

		if (TYPE == ActivityType.COMMENT) {
			accumulator.setOutputType(StatisticsOutput.OutputType.COMMENT_COUNT);
		} else if (TYPE == ActivityType.REPLY) {
			accumulator.setOutputType(StatisticsOutput.OutputType.REPLY_COUNT);
		}

		return accumulator;
	}

	@Override
	public StatisticsOutput add(PostActivity activity, StatisticsOutput accumulator) {
		if (accumulator.getPostId() == null) {
			accumulator.setPostId(activity.getPostId());
		}

		if (TYPE == activity.getType()) {
			accumulator.incrementValue();
		}

		return accumulator;
	}

	@Override
	public StatisticsOutput getResult(StatisticsOutput accumulator) {
		return accumulator;
	}

	@Override
	public StatisticsOutput merge(StatisticsOutput a, StatisticsOutput b) {
		Long postId = (a.getPostId() != null) ? a.getPostId() : b.getPostId();

		StatisticsOutput accumulator = new StatisticsOutput();
		accumulator.setPostId(postId);
		accumulator.setValue(a.getValue() + b.getValue());

		return accumulator;
	}

}
