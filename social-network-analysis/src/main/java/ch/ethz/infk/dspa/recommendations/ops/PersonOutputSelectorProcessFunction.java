package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class PersonOutputSelectorProcessFunction extends KeyedProcessFunction<Long, PersonActivity, PersonActivity> {

	private static final long serialVersionUID = 1L;

	public static OutputTag<PersonActivity> SELECTED = new OutputTag<>("selected",
			TypeInformation.of(PersonActivity.class));

	private transient ValueState<Integer> count;

	// TODO [rsc] Issue #19 Implement more meaningful selection of 10 people

	@Override
	public void processElement(PersonActivity activity, Context ctx, Collector<PersonActivity> out)
			throws Exception {

		if (count.value() == null) {
			count.update(0);
		}

		if (count.value() == 0) {
			// register reset
			ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 59 * 60 * 1000);
		}

		if (count.value() < 9) {
			ctx.output(SELECTED, activity);
		}

		count.update(count.value() + 1);
		out.collect(activity);

	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<PersonActivity> out) throws Exception {
		super.onTimer(timestamp, ctx, out);
		count.update(0);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("count",
				TypeInformation.of(Integer.class));
		this.count = getRuntimeContext().getState(descriptor);

	}

}
