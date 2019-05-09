package ch.ethz.infk.dspa.anomalies.dto;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;

public class Feature {

	public enum FeatureId {
		TIMESTAMP,
		CONTENTS_SHORT,
		CONTENTS_MEDIUM,
		CONTENTS_LONG,
	}

	public enum EventType {
		POST,
		COMMENT,
		LIKE
	}

	private FeatureId featureId;
	private Double featureValue;

	// events
	private Post post = null;
	private Comment comment = null;
	private Like like = null;

	public Feature withEvent(Post post) {
		if (comment != null || like != null) {
			throw new IllegalArgumentException("Event already set");
		}
		this.post = post;
		return this;
	}

	public Feature withEvent(Comment comment) {
		if (post != null || like != null) {
			throw new IllegalArgumentException("Event already set");
		}
		this.comment = comment;
		return this;
	}

	public Feature withEvent(Like like) {
		if (post != null || comment != null) {
			throw new IllegalArgumentException("Event already set");
		}
		this.like = like;
		return this;
	}

	public Feature withFeatureId(FeatureId featureId) {
		this.featureId = featureId;
		return this;
	}

	public Feature withFeatureValue(Double featureValue) {
		this.featureValue = featureValue;
		return this;
	}

	public FeatureId getFeatureId() {
		return this.featureId;
	}

	public Double getFeatureValue() {
		return this.featureValue;
	}

	public EventType getEventType() {
		if (post != null) {
			return EventType.POST;
		}

		if (comment != null) {
			return EventType.COMMENT;
		}

		if (like != null) {
			return EventType.LIKE;
		}

		throw new IllegalArgumentException("No event set in feature");
	}

	public Long getPersonId() {
		if (post != null) {
			return post.getPersonId();
		}

		if (comment != null) {
			return comment.getPersonId();
		}

		if (like != null) {
			return like.getPersonId();
		}

		throw new IllegalArgumentException("No event set in feature");
	}

	public String getEventId() {
		if (post != null) {
			return String.valueOf(post.getId());
		}

		if (comment != null) {
			return String.valueOf(comment.getId());
		}

		if (like != null) {
			return String.join("_", String.valueOf(like.getPersonId()), String.valueOf(like.getPostId()));
		}

		throw new IllegalArgumentException("No event set in feature");

	}

	public Post getPost() {
		return post;
	}

	public Comment getComment() {
		return comment;
	}

	public Like getLike() {
		return like;
	}

	public String getGUID() {
		return getEventType() + "_" + getEventId();
	}

	public static Feature of(Post post) {
		return new Feature()
				.withEvent(post);
	}

	public static Feature of(Comment comment) {
		return new Feature()
				.withEvent(comment);
	}

	public static Feature of(Like like) {
		return new Feature()
				.withEvent(like);
	}
}
