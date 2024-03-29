package ch.ethz.infk.dspa.anomalies.dto;

import org.joda.time.DateTime;

import com.google.common.base.Objects;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;

public class Feature {

	public enum FeatureId {
		TIMESPAN(0),
		CONTENTS_SHORT(1),
		CONTENTS_MEDIUM(2),
		CONTENTS_LONG(3),
		TAG_COUNT(4),
		NEW_USER_LIKES(5),
		INTERACTIONS_RATIO(6);

		private final int id;

		FeatureId(int id) {
			this.id = id;
		}

		public int getId() {
			return this.id;
		}
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

	public static Feature copy(Feature feature) {

		return new Feature().withEvent(feature.getPost())
				.withEvent(feature.getComment())
				.withEvent(feature.getLike())
				.withFeatureId(feature.getFeatureId())
				.withFeatureValue(feature.getFeatureValue());
	}

	public Feature withEvent(Post post) {
		if (post != null && (comment != null || like != null)) {
			throw new IllegalArgumentException("Event already set");
		}
		this.post = post;
		return this;
	}

	public Feature withEvent(Comment comment) {
		if (comment != null && (post != null || like != null)) {
			throw new IllegalArgumentException("Event already set");
		}
		this.comment = comment;
		return this;
	}

	public Feature withEvent(Like like) {
		if (like != null && (post != null || comment != null)) {
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

	public Long getPostId() {
		if (post != null) {
			return post.getId();
		}

		if (comment != null) {
			return comment.getReplyToPostId();
		}

		if (like != null) {
			return like.getPostId();
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

	public DateTime getCreationDate() {
		if (post != null) {
			return post.getCreationDate();
		}

		if (comment != null) {
			return comment.getCreationDate();
		}

		if (like != null) {
			return like.getCreationDate();
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

	@Override
	public String toString() {
		return "Feature{" +
				"featureId=" + featureId +
				", featureValue=" + featureValue +
				", postId=" + getPostId() +
				", eventId=" + getGUID() +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Feature feature = (Feature) o;
		return featureId == feature.featureId &&
				Objects.equal(featureValue, feature.featureValue) &&
				Objects.equal(getPostId(), feature.getPostId()) &&
				Objects.equal(getGUID(), feature.getGUID());
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(featureId.name(), featureValue, getPostId(), getGUID());
	}
}
