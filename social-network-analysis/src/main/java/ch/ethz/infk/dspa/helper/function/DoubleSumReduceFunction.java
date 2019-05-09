package ch.ethz.infk.dspa.helper.function;

import org.apache.flink.api.common.functions.ReduceFunction;

public class DoubleSumReduceFunction implements ReduceFunction<Double> {

	private static final long serialVersionUID = 1L;

	@Override
	public Double reduce(Double value1, Double value2) throws Exception {
		return value1 + value2;
	}

}
