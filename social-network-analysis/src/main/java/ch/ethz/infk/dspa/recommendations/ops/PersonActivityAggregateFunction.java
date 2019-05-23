package ch.ethz.infk.dspa.recommendations.ops;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.AggregateFunction;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity.PersonActivityType;

public class PersonActivityAggregateFunction implements AggregateFunction<PersonActivity, PersonActivity, PersonActivity> {

	private static final long serialVersionUID = 1L;

	@Override
	public PersonActivity createAccumulator() {
		PersonActivity p = new PersonActivity();
		p.setType(PersonActivityType.STATIC);
		return p;
	}

	@Override
	public PersonActivity add(PersonActivity element, PersonActivity accumulator) {

		if (element.getCategoryMap() != null) {
			Map<String, Integer> categoryMap = new HashMap<>(accumulator.getCategoryMap());
			element.getCategoryMap().forEach((key, value) -> categoryMap.merge(key, value, Integer::sum));
			accumulator.setCategoryMap(categoryMap);
		}

		if (element.getType() == null || element.getType() != PersonActivityType.STATIC) {
			accumulator.setType(null);
		}

		accumulator.setPersonId(element.getPersonId());
		return accumulator;
	}

	@Override
	public PersonActivity getResult(PersonActivity accumulator) {
		return accumulator;
	}

	@Override
	public PersonActivity merge(PersonActivity a, PersonActivity b) {
		PersonActivity activity = createAccumulator();
		activity = add(a, activity);
		activity = add(b, activity);
		return activity;
	}

}
