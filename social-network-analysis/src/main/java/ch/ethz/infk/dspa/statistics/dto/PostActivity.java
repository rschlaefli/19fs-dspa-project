package ch.ethz.infk.dspa.statistics.dto;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;

public class PostActivity {

	public enum ActivityType {
		POST,
		LIKE,
		COMMENT,
		REPLY
	}

	private Long postId;
	private ActivityType type;
	private Long personId;

	public PostActivity(Long postId, ActivityType type, Long personId) {
		this.postId = postId;
		this.type = type;
		this.personId = personId;
	}

	public Long getPostId() {
		return postId;
	}

	public ActivityType getType() {
		return type;
	}

	public Long getPersonId() {
		return personId;
	}

	public static PostActivity of(Post post) {
		return new PostActivity(post.getId(), ActivityType.POST, post.getPersonId());
	}

	public static PostActivity of(Comment comment) {
		ActivityType type = comment.getReplyToCommentId() == null ? ActivityType.COMMENT : ActivityType.REPLY;
		return new PostActivity(comment.getReplyToPostId(), type, comment.getPersonId());
	}

	public static PostActivity of(Like like) {
		return new PostActivity(like.getPostId(), ActivityType.LIKE, like.getPersonId());
	}
}
