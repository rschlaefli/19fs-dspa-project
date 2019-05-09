package ch.ethz.infk.dspa.statistics.dto;

import ch.ethz.infk.dspa.statistics.dto.PostActivity.ActivityType;

public class PostActivityCount {

	private Long postId;
	private ActivityType type;
	private long count;

	public PostActivityCount() {
	}

	public PostActivityCount(long postId, ActivityType type) {
		super();
		this.postId = postId;
		this.type = type;
	}

	public static String csvHeader() {
		return "postId;activityType;count";
	}

	public Long getPostId() {
		return postId;
	}

	public void setPostId(long postId) {
		this.postId = postId;
	}

	public ActivityType getType() {
		return type;
	}

	public void setType(ActivityType type) {
		this.type = type;
	}

	public void incrementCount() {
		this.count += 1;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public String toCsv() {
		return String.format("%d;%s;%d", postId, type, count);
	}

}
