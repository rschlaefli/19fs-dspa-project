package ch.ethz.infk.dspa.recommendations.ops;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FilterFunction;

import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;

public class FriendsFilterFunction extends AbstractRichFunction implements FilterFunction<PersonSimilarity> {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(PersonSimilarity value) throws Exception {
		// TODO [rsc] implement
		return true;
	}

}
