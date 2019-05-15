package ch.ethz.infk.dspa.stream.dto;

public class CommentPostMapping {

	private Long commentId;
	private Long parentCommentId;
	private Long parentPostId;

	public Long getCommentId() {
		return commentId;
	}

	public void setCommentId(Long commentId) {
		this.commentId = commentId;
	}

	public Long getParentCommentId() {
		return parentCommentId;
	}

	public void setParentCommentId(Long parentCommentId) {
		this.parentCommentId = parentCommentId;
	}

	public Long getParentPostId() {
		return parentPostId;
	}

	public void setParentPostId(Long parentPostId) {
		this.parentPostId = parentPostId;
	}

	public static CommentPostMapping of(Long commentId, Long parentCommentId, Long parentPostId) {
		CommentPostMapping m = new CommentPostMapping();
		m.setCommentId(commentId);
		m.setParentCommentId(parentCommentId);
		m.setParentPostId(parentPostId);
		return m;
	}
}
