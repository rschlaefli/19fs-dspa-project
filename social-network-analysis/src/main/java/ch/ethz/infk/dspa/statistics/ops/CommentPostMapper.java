package ch.ethz.infk.dspa.statistics.ops;

import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.functions.KeySelector;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;

public class CommentPostMapper extends RichMapFunction<Comment, PostActivity> {

	private static final long serialVersionUID = 1L;
	private final static Logger LOG = LogManager.getLogger();

	private transient ValueState<Long> postId;

	@Override
	public PostActivity map(Comment comment) throws Exception {

		ActivityType type = null;

		if (comment.getReplyToPostId() != null) {
			// comment of post
			postId.update(comment.getReplyToPostId());
			type = ActivityType.COMMENT;
		} else if (comment.getReplyToCommentId() != null && postId.value() != null) {
			// reply to comment
			type = ActivityType.REPLY;
		} else {
			LOG.warn("Cannot resolve post id of comment: {}", comment);
		}

		return new PostActivity(postId.value(), type, comment.getPersonId());
	}

	public static class CommentKeySelector implements KeySelector<Comment, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(Comment comment) throws Exception {
			if (comment.getReplyToCommentId() == null) {
				return comment.getId();
			} else {
				return comment.getReplyToCommentId();
			}
		}
	}

}
