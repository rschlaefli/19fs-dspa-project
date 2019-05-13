package ch.ethz.infk.dspa.statistics.dto;

import ch.ethz.infk.dspa.avro.Post;
import com.google.common.base.Objects;
import org.apache.zookeeper.data.Stat;

public class StatisticsOutput {

    public enum OutputType {
        COMMENT_COUNT,
        REPLY_COUNT,
        UNIQUE_PERSON_COUNT
    }

    private Long timestamp;
    private Long postId;
    private Long value = 0L;
    private PostActivity.ActivityType activityType;
    private OutputType outputType;

    public StatisticsOutput() {}

    public StatisticsOutput(Long timestamp, Long postId, Long value, OutputType outputType) {
        this.timestamp = timestamp;
        this.postId = postId;
        this.value = value;
        this.outputType = outputType;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getPostId() {
        return postId;
    }

    public void setPostId(Long postId) {
        this.postId = postId;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public void incrementValue() {
        this.value += 1;
    }

    public PostActivity.ActivityType getActivityType() {
        return activityType;
    }

    public void setActivityType(PostActivity.ActivityType activityType) {
        this.activityType = activityType;
    }

    public OutputType getOutputType() {
        return outputType;
    }

    public void setOutputType(OutputType outputType) {
        this.outputType = outputType;
    }

    @Override
    public String toString() {
        return "StatisticsOutput{" +
                "timestamp=" + timestamp +
                ", postId=" + postId +
                ", value=" + value +
                ", activityType=" + activityType +
                ", outputType=" + outputType +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatisticsOutput that = (StatisticsOutput) o;
        return Objects.equal(timestamp, that.timestamp) &&
                Objects.equal(postId, that.postId) &&
                Objects.equal(value, that.value) &&
                outputType == that.outputType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(timestamp, postId, value, outputType);
    }
}
