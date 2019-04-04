package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class PersonOutputSelectorProcessFunction extends ProcessFunction<PersonActivity, PersonActivity> {

	private static final long serialVersionUID = 1L;

	public static OutputTag<PersonActivity> selected = new OutputTag<>("selected");

	@Override
	public void processElement(PersonActivity activity, Context ctx, Collector<PersonActivity> out)
			throws Exception {
		// TODO write side output

		if (true) {
			ctx.output(selected, activity);
		}

		out.collect(activity);

	}

}
