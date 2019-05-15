package ch.ethz.infk.dspa.recommendations.dto;

import java.util.List;

public class FriendsRecommendation {

	private Long personId;

	private List<SimilarityTuple> similarities;

	public Long getPersonId() {
		return personId;
	}

	public void setPersonId(long personId) {
		this.personId = personId;
	}

	public List<SimilarityTuple> getSimilarities() {
		return similarities;
	}

	public void setSimilarities(List<SimilarityTuple> similarities) {
		this.similarities = similarities;
	}

	public static class SimilarityTuple {
		private Long personId;
		private Double similarity;

		public SimilarityTuple() {
		};

		public SimilarityTuple(Long personId, Double similarity) {
			this.personId = personId;
			this.similarity = similarity;
		}

		public Long getPersonId() {
			return personId;
		}

		public void setPersonId(Long personId) {
			this.personId = personId;
		}

		public Double getSimilarity() {
			return similarity;
		}

		public void setSimilarity(Double similarity) {
			this.similarity = similarity;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (personId ^ (personId >>> 32));
			long temp;
			temp = Double.doubleToLongBits(similarity);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			SimilarityTuple other = (SimilarityTuple) obj;
			if (personId != other.personId) return false;
			if (Double.doubleToLongBits(similarity) != Double.doubleToLongBits(other.similarity)) return false;
			return true;
		}

		@Override
		public String toString() {
			return "SimilarityTuple [personId=" + personId + ", similarity=" + similarity + "]";
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (personId ^ (personId >>> 32));
		result = prime * result + ((similarities == null) ? 0 : similarities.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		FriendsRecommendation other = (FriendsRecommendation) obj;
		if (personId != other.personId) return false;
		if (similarities == null) {
			if (other.similarities != null) return false;
		} else if (!similarities.equals(other.similarities)) return false;
		return true;
	}

	@Override
	public String toString() {
		return "FriendsRecommendation [personId=" + personId + ", similarities=" + similarities + "]";
	}

}
