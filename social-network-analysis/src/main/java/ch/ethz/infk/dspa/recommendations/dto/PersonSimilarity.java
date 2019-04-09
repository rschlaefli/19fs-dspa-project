package ch.ethz.infk.dspa.recommendations.dto;

public class PersonSimilarity {

	private static final long serialVersionUID = 1L;

	private Long person1Id;
	private Long person2Id;
	private Double similarity;

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

}
