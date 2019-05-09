package ch.ethz.infk.dspa.helper.function;

import org.apache.flink.api.common.functions.ReduceFunction;

public class LongSumReduceFunction implements ReduceFunction<Long> {

	private static final long serialVersionUID = 1L;

	@Override
	public Long reduce(Long value1, Long value2) throws Exception {
		return value1 + value2;
	}

}
