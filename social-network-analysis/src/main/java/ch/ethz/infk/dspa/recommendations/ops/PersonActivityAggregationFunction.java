package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class PersonActivityAggregationFunction
		implements AggregateFunction<PersonActivity, PersonActivity, PersonActivity> {

	private static final long serialVersionUID = 1L;

	@Override
	public PersonActivity createAccumulator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersonActivity add(PersonActivity value, PersonActivity accumulator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersonActivity getResult(PersonActivity accumulator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersonActivity merge(PersonActivity a, PersonActivity b) {
		// TODO Auto-generated method stub
		return null;
	}

}
