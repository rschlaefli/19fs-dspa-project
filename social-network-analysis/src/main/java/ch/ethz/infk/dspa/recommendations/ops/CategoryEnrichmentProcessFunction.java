package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class CategoryEnrichmentProcessFunction extends KeyedProcessFunction<Long, PersonActivity, PersonActivity> {

	private static final long serialVersionUID = 1L;

	@Override
	public void processElement(PersonActivity in, Context ctx, Collector<PersonActivity> out)
			throws Exception {
		// TODO Auto-generated method stub

	}

}
