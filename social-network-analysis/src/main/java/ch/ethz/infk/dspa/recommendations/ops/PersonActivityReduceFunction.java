package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.api.common.functions.ReduceFunction;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class PersonActivityReduceFunction implements ReduceFunction<PersonActivity> {

	private static final long serialVersionUID = 1L;

	@Override
	public PersonActivity reduce(PersonActivity activity1, PersonActivity activity2) throws Exception {
		activity1.setPostId(null);
		activity1.mergeCategoryMap(activity2.categoryMap());
		return activity1;
	}
}