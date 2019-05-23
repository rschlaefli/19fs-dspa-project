package ch.ethz.infk.dspa.recommendations.ops;

import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation;
import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation.SimilarityTuple;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarityComparator;

public class TopKAggregateFunction
		implements AggregateFunction<PersonSimilarity, PriorityQueue<PersonSimilarity>, FriendsRecommendation> {

	private static final long serialVersionUID = 1L;

	private final int k;

	public TopKAggregateFunction(int k) {
		this.k = k;
	}

	@Override
	public PriorityQueue<PersonSimilarity> createAccumulator() {

		return new PriorityQueue<PersonSimilarity>(this.k, new PersonSimilarityComparator());
	}

	@Override
	public PriorityQueue<PersonSimilarity> add(PersonSimilarity value, PriorityQueue<PersonSimilarity> accumulator) {

		if (accumulator.size() < this.k) {
			// less that k elements => add
			accumulator.add(value);
		} else {
			PersonSimilarity head = accumulator.peek();

			if (new PersonSimilarityComparator().reversed().compare(head, value) > 0) {
				accumulator.poll(); // remove head
				accumulator.add(value); // add new larger
			}
		}
		return accumulator;
	}

	@Override
	public FriendsRecommendation getResult(PriorityQueue<PersonSimilarity> accumulator) {

		PersonSimilarity similarity = accumulator.peek();
		Long personId = similarity.person1Id();
		boolean onlyStatic = similarity.person1OnlyStatic();

		List<SimilarityTuple> topkSimilarities = accumulator.stream()
				.sorted(new PersonSimilarityComparator().reversed())
				.map(x -> new SimilarityTuple(x.person2Id(), x.similarity(), x.getCategoryMap2()))
				.collect(Collectors.toList());

		FriendsRecommendation recommendation = new FriendsRecommendation();
		recommendation.setPersonId(personId);
		recommendation.setSimilarities(topkSimilarities);
		recommendation.setInactive(onlyStatic);
		recommendation.setCategoryMap(similarity.getCategoryMap1());

		return recommendation;
	}

	@Override
	public PriorityQueue<PersonSimilarity> merge(PriorityQueue<PersonSimilarity> q1,
			PriorityQueue<PersonSimilarity> q2) {

		q1.addAll(q2);
		int size = q1.size();

		// remove smallest size - k elements
		for (int i = 0; i < size - this.k; i++) {
			q1.remove();
		}

		return q1;
	}

}
