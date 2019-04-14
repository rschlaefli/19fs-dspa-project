package ch.ethz.infk.dspa.recommendations.ops;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;

public class TopKAggregateFunction
		implements AggregateFunction<PersonSimilarity, PriorityQueue<PersonSimilarity>, List<PersonSimilarity>> {

	private static final long serialVersionUID = 1L;

	private final int k; // TODO [nku] need to checkpoint?

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
			if (head.similarity() < value.similarity()) {
				accumulator.poll(); // remove head
				accumulator.add(value); // add new larger
			}
		}
		return accumulator;
	}

	@Override
	public List<PersonSimilarity> getResult(PriorityQueue<PersonSimilarity> accumulator) {
		List<PersonSimilarity> list = accumulator.stream().collect(Collectors.toList());

		Comparator<PersonSimilarity> cmp = new Comparator<PersonSimilarity>() {
			@Override
			public int compare(PersonSimilarity o1, PersonSimilarity o2) {
				return Double.compare(o1.similarity(), o2.similarity());
			}
		};

		// sort in descending order by similarity
		Collections.sort(list, Collections.reverseOrder(cmp));

		return list;
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
