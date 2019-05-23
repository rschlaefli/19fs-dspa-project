package ch.ethz.infk.dspa.recommendations.dto;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FriendsRecommendation implements Comparable<FriendsRecommendation> {

	private Long personId;

	// for this recommendation the person did not have any events
	private boolean inactive;

	private Map<String, Integer> categoryMap;

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
		Collections.sort(similarities);
		this.similarities = similarities;
	}

	public boolean isInactive() {
		return inactive;
	}

	public void setInactive(boolean inactive) {
		this.inactive = inactive;
	}

	public Map<String, Integer> getCategoryMap() {
		return categoryMap;
	}

	public void setCategoryMap(Map<String, Integer> categoryMap) {
		this.categoryMap = categoryMap;
	}

	public static String formatCategoryMap(Map<String, Integer> map) {
		if (map == null) {
			return "[]";
		}
		return map.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).sorted().collect(Collectors.toList())
				.toString();
	}

	@Override
	public String toString() {

		return "FriendsRecommendation [personId=" + personId + ", inactive=" + inactive + ", categoryMap="
				+ formatCategoryMap(categoryMap)
				+ ", similarities=" + similarities + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((categoryMap == null) ? 0 : categoryMap.hashCode());
		result = prime * result + (inactive ? 1231 : 1237);
		result = prime * result + ((personId == null) ? 0 : personId.hashCode());
		result = prime * result + ((similarities == null) ? 0 : similarities.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		FriendsRecommendation other = (FriendsRecommendation) obj;
		if (categoryMap == null) {
			if (other.categoryMap != null) return false;
		} else if (!categoryMap.equals(other.categoryMap)) return false;
		if (inactive != other.inactive) return false;
		if (personId == null) {
			if (other.personId != null) return false;
		} else if (!personId.equals(other.personId)) return false;
		if (similarities == null) {
			if (other.similarities != null) return false;
		} else if (!similarities.equals(other.similarities)) return false;
		return true;
	}

	public static class SimilarityTuple implements Comparable<SimilarityTuple> {
		private Long personId;
		private Double similarity;

		private Map<String, Integer> categoryMap;

		public SimilarityTuple() {
		};

		public SimilarityTuple(Long personId, Double similarity, Map<String, Integer> categoryMap) {
			this.personId = personId;
			this.similarity = similarity;
			this.categoryMap = categoryMap;
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

		public Map<String, Integer> getCategoryMap() {
			return categoryMap;
		}

		public void setCategoryMap(Map<String, Integer> categoryMap) {
			this.categoryMap = categoryMap;
		}

		@Override
		public String toString() {
			return "SimilarityTuple [personId=" + personId + ", similarity=" + similarity + ", categoryMap="
					+ formatCategoryMap(categoryMap) + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((categoryMap == null) ? 0 : categoryMap.hashCode());
			result = prime * result + ((personId == null) ? 0 : personId.hashCode());
			result = prime * result + ((similarity == null) ? 0 : similarity.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			SimilarityTuple other = (SimilarityTuple) obj;
			if (categoryMap == null) {
				if (other.categoryMap != null) return false;
			} else if (!categoryMap.equals(other.categoryMap)) return false;
			if (personId == null) {
				if (other.personId != null) return false;
			} else if (!personId.equals(other.personId)) return false;
			if (similarity == null) {
				if (other.similarity != null) return false;
			} else if (!similarity.equals(other.similarity)) return false;
			return true;
		}

		@Override
		public int compareTo(SimilarityTuple o) {

			int c = -1 * similarity.compareTo(o.getSimilarity());
			if (c == 0) {
				c = getPersonId().compareTo(o.getPersonId());
			}

			return c;
		}

	}

	@Override
	public int compareTo(FriendsRecommendation o) {
		return this.getPersonId().compareTo(o.getPersonId());
	}

}
