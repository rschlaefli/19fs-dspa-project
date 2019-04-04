package ch.ethz.infk.dspa.recommendations.ops;

import java.util.List;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;

public class TopKAggregateFunction
		implements AggregateFunction<PersonSimilarity, List<PersonSimilarity>, List<PersonSimilarity>> {

	private static final long serialVersionUID = 1L;

	private final int k;

	public TopKAggregateFunction(int k) {
		this.k = k;
	}

	@Override
	public List<PersonSimilarity> createAccumulator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PersonSimilarity> add(PersonSimilarity value, List<PersonSimilarity> accumulator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PersonSimilarity> getResult(List<PersonSimilarity> accumulator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PersonSimilarity> merge(List<PersonSimilarity> a, List<PersonSimilarity> b) {
		// TODO Auto-generated method stub
		return null;
	}

}
