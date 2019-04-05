package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.api.common.functions.MapFunction;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.recommendations.dto.Category;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class CommentToPersonActivityMapFunction implements MapFunction<Comment, PersonActivity> {

	private static final long serialVersionUID = 1L;

	@Override
	public PersonActivity map(Comment comment) throws Exception {

		PersonActivity activity = new PersonActivity();
		activity.setPostId(comment.getReplyToPostId());
		activity.setPersonId(comment.getPersonId());

		// TODO [nku] check if want to keep creationTime in PersonActivity

		Long placeId = comment.getPlaceId();
		if (placeId != null) {
			activity.countCategory(Category.place(placeId));
		}

		// TODO potentially add content topic extraction

		return activity;
	}

}
