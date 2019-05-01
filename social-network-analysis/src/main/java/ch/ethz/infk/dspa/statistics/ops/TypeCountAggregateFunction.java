package ch.ethz.infk.dspa.statistics.ops;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;
import ch.ethz.infk.dspa.statistics.dto.PostActivityCount;

public class TypeCountAggregateFunction
		implements AggregateFunction<PostActivity, PostActivityCount, PostActivityCount> {

	private static final long serialVersionUID = 1L;
	private final ActivityType TYPE;

	public TypeCountAggregateFunction(ActivityType type) {
		this.TYPE = type;
	}

	@Override
	public PostActivityCount createAccumulator() {
		PostActivityCount accumulator = new PostActivityCount();
		accumulator.setType(TYPE);
		return accumulator;
	}

	@Override
	public PostActivityCount add(PostActivity activity, PostActivityCount accumulator) {

		if (accumulator.getPostId() == null) {
			accumulator.setPostId(activity.getPostId());
		}

		if (TYPE == activity.getType()) {
			accumulator.incrementCount();
		}

		return accumulator;
	}

	@Override
	public PostActivityCount getResult(PostActivityCount accumulator) {
		return accumulator;
	}

	@Override
	public PostActivityCount merge(PostActivityCount a, PostActivityCount b) {

		Long postId = (a.getPostId() != null) ? a.getPostId() : b.getPostId();

		PostActivityCount accumulator = new PostActivityCount(postId, TYPE);
		accumulator.setCount(a.getCount() + b.getCount());

		return accumulator;
	}

}
