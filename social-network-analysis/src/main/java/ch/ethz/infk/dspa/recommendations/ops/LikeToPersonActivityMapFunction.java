package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;

import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class LikeToPersonActivityMapFunction extends AbstractRichFunction implements MapFunction<Like, PersonActivity> {

	private static final long serialVersionUID = 1L;

	@Override
	public void open(Configuration parameters) throws Exception {
		// TODO Auto-generated method stub
		super.open(parameters);
	}

	@Override
	public PersonActivity map(Like value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
