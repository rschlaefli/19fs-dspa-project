package ch.ethz.infk.dspa.recommendations.dto;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PersonSimilarity {

	private Long person1Id;
	private Long person2Id;
	private Double similarity;

	public PersonSimilarity() {

	}

	public PersonSimilarity(Long p1, Long p2) {
		this.person1Id = p1;
		this.person2Id = p2;
	}

	public Long person1Id() {
		return person1Id;
	}

	public Long person2Id() {
		return person2Id;
	}

	public Double similarity() {
		return similarity;
	}

	public PersonSimilarity withPerson1Id(Long id) {
		this.person1Id = id;
		return this;
	}

	public PersonSimilarity withPerson2Id(Long id) {
		this.person2Id = id;
		return this;
	}

	public PersonSimilarity withSimilarity(Double similarity) {
		this.similarity = similarity;
		return this;
	}

	@Override
	public String toString() {
		return "PersonSimilarity [person1Id=" + person1Id + ", person2Id=" + person2Id + ", similarity=" + similarity
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((person1Id == null) ? 0 : person1Id.hashCode());
		result = prime * result + ((person2Id == null) ? 0 : person2Id.hashCode());
		result = prime * result + ((similarity == null) ? 0 : similarity.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		PersonSimilarity other = (PersonSimilarity) obj;
		if (person1Id == null) {
			if (other.person1Id != null) return false;
		} else if (!person1Id.equals(other.person1Id)) return false;
		if (person2Id == null) {
			if (other.person2Id != null) return false;
		} else if (!person2Id.equals(other.person2Id)) return false;
		if (similarity == null) {
			return other.similarity == null;
		} else
			return similarity.equals(other.similarity);
	}

	public static PersonSimilarity dotProduct(PersonActivity activity1, PersonActivity activity2) {
		Map<String, Integer> firstMap = activity1.categoryMap();
		Map<String, Integer> secondMap = activity2.categoryMap();

		Set<String> keys = new HashSet<>();
		keys.addAll(firstMap.keySet());
		keys.addAll(secondMap.keySet());

		double sum = 0;

		for (String key : keys) {
			sum += firstMap.getOrDefault(key, 0) * secondMap.getOrDefault(key, 0);
		}

		return new PersonSimilarity()
				.withPerson1Id(activity1.personId())
				.withPerson2Id(activity2.personId())
				.withSimilarity(sum);
	}

}
