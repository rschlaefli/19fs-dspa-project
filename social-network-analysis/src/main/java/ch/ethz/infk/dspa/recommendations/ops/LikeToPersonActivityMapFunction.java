package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.api.common.functions.MapFunction;

import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class LikeToPersonActivityMapFunction implements MapFunction<Like, PersonActivity> {

	private static final long serialVersionUID = 1L;

	@Override
	public PersonActivity map(Like like) throws Exception {

		PersonActivity activity = new PersonActivity();
		activity.setPostId(like.getPostId());
		activity.setPersonId(like.getPersonId());

		// TODO [nku] check if want to keep creationTime in PersonActivity

		return activity;
	}

}
