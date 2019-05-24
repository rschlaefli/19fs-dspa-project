package ch.ethz.infk.dspa.statistics.dto;

public class StatisticsOutput {

	public enum OutputType {
		COMMENT_COUNT,
		REPLY_COUNT,
		UNIQUE_PERSON_COUNT
	}

	private Long postId;
	private Long value = 0L;
	private OutputType outputType;

	public StatisticsOutput() {
	}

	public StatisticsOutput(Long postId, Long value, OutputType outputType) {
		this.postId = postId;
		this.value = value;
		this.outputType = outputType;
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

	public OutputType getOutputType() {
		return outputType;
	}

	public void setOutputType(OutputType outputType) {
		this.outputType = outputType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((outputType == null) ? 0 : outputType.hashCode());
		result = prime * result + ((postId == null) ? 0 : postId.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		StatisticsOutput other = (StatisticsOutput) obj;
		if (outputType != other.outputType) return false;
		if (postId == null) {
			if (other.postId != null) return false;
		} else if (!postId.equals(other.postId)) return false;
		if (value == null) {
			if (other.value != null) return false;
		} else if (!value.equals(other.value)) return false;
		return true;
	}

	@Override
	public String toString() {
		return "StatisticsOutput [postId=" + postId + ", value=" + value + ", outputType=" + outputType + "]";
	}

}
