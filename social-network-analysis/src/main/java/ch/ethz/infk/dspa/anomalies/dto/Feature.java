package ch.ethz.infk.dspa.anomalies.dto;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import org.joda.time.DateTime;

public class Feature<T> {

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

    private Long personId;
    private String eventId;
    private EventType eventType;
    private T event;

    private FeatureId featureId;
    private Double featureValue;

    public Feature<T> withEventType(EventType eventType) {
        this.eventType = eventType;
        return this;
    }

    public Feature<T> withPersonId(Long personId) {
        this.personId = personId;
        return this;
    }

    public Feature<T> withEventId(String eventId) {
        this.eventId = eventId;
        return this;
    }

    public Feature<T> withEvent(T event) {
        this.event = event;
        return this;
    }

    public Feature<T> withFeatureId(FeatureId featureId) {
        this.featureId = featureId;
        return this;
    }

    public Feature<T> withFeatureValue(Double featureValue) {
        this.featureValue = featureValue;
        return this;
    }

    public Long getPersonId() {
        return this.personId;
    }

    public String getEventId() {
        return this.eventId;
    }

    public EventType getEventType() {
        return this.eventType;
    }

    public FeatureId getFeatureId() {
        return this.featureId;
    }

    public T getEvent() {
        return this.event;
    }

    public Double getFeatureValue() {
        return this.featureValue;
    }

    public String getGUID() {
        return this.eventType + "_" + this.eventId;
    }

    public boolean hasEventTypeWithContents() {
        return this.eventType == EventType.POST || this.eventType == EventType.COMMENT;
    }

    public static Feature of(Post post) {
        return new Feature<Post>()
                .withEventType(EventType.POST)
                .withPersonId(post.getPersonId())
                .withEventId(post.getId().toString())
                .withEvent(post);
    }

    public static Feature of(Comment comment) {
        return new Feature<Comment>()
                .withEventType(EventType.COMMENT)
                .withPersonId(comment.getPersonId())
                .withEventId(comment.getReplyToPostId().toString())
                .withEvent(comment);
    }

    public static Feature of(Like like) {
        return new Feature<Like>()
                .withEventType(EventType.LIKE)
                .withPersonId(like.getPersonId())
                .withEventId(like.getPersonId() + "_" + like.getPostId())
                .withEvent(like);
    }
}
