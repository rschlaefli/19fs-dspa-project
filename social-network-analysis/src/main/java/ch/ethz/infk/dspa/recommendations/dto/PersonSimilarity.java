package ch.ethz.infk.dspa.recommendations.dto;

import org.apache.flink.api.java.tuple.Tuple3;

public class PersonSimilarity extends Tuple3<Long, Long, Double> {

	private static final long serialVersionUID = 1L;

	public Long person1Id() {
		return f0;
	}

	public Long person2Id() {
		return f1;
	}

	public Double similarity() {
		return f2;
	}

	public PersonSimilarity withPerson1Id(Long id) {
		f0 = id;
		return this;
	}

	public PersonSimilarity withPerson2Id(Long id) {
		f1 = id;
		return this;
	}

	public PersonSimilarity withSimilarity(Double similarity) {
		f2 = similarity;
		return this;
	}

}
