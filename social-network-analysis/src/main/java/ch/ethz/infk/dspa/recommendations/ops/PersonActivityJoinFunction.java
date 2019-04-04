package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.api.common.functions.JoinFunction;

import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity;

public class PersonActivityJoinFunction implements JoinFunction<PostActivity, PostActivity, PersonSimilarity> {

	private static final long serialVersionUID = 1L;

	@Override
	public PersonSimilarity join(PostActivity first, PostActivity second) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
