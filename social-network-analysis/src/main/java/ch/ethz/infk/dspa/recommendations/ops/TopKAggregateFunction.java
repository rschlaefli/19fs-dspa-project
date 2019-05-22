package ch.ethz.infk.dspa.recommendations.ops;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation;
import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation.SimilarityTuple;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;

public class TopKAggregateFunction
		implements AggregateFunction<PersonSimilarity, PriorityQueue<PersonSimilarity>, FriendsRecommendation> {

	private static final long serialVersionUID = 1L;

	private final int k;

	public TopKAggregateFunction(int k) {
		this.k = k;
	}

	@Override
	public PriorityQueue<PersonSimilarity> createAccumulator() {

		Comparator<PersonSimilarity> comp = new Comparator<PersonSimilarity>() {
			@Override
			public int compare(PersonSimilarity o1, PersonSimilarity o2) {
				return Double.compare(o1.similarity(), o2.similarity());
			}
		};

		return new PriorityQueue<PersonSimilarity>(this.k, comp);
	}

	@Override
	public PriorityQueue<PersonSimilarity> add(PersonSimilarity value, PriorityQueue<PersonSimilarity> accumulator) {

		if (accumulator.size() < this.k) {
			// less that k elements => add
			accumulator.add(value);
		} else {
			PersonSimilarity head = accumulator.peek();

			// TODO [nku] refactor
			if (head.similarity() < value.similarity()
					|| (head.similarity() == value.similarity() && head.person2Id() < value.person2Id())) {
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

		// TODO [nku] refactor
		Comparator<PersonSimilarity> comp = new Comparator<PersonSimilarity>() {

			@Override
			public int compare(PersonSimilarity o1, PersonSimilarity o2) {
				int c = o1.similarity().compareTo(o2.similarity());
				if (c != 0) {
					return c;
				} else {
					return o1.person2Id().compareTo(o2.person2Id());
				}
			}

		};

		List<SimilarityTuple> topkSimilarities = accumulator.stream()
				// .sorted(Comparator.comparingDouble(PersonSimilarity::similarity).reversed())
				.sorted(comp.reversed())
				.map(x -> new SimilarityTuple(x.person2Id(), x.similarity()))
				.collect(Collectors.toList());

		FriendsRecommendation recommendation = new FriendsRecommendation();
		recommendation.setPersonId(personId);
		recommendation.setSimilarities(topkSimilarities);
		recommendation.setInactive(onlyStatic);

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
