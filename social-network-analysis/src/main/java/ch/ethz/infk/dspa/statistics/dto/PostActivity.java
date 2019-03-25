package ch.ethz.infk.dspa.statistics.dto;

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

}
