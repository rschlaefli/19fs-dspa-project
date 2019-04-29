package ch.ethz.infk.dspa.recommendations.ops;

import ch.ethz.infk.dspa.helper.StaticDataParser;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FilterFunction;

import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import java.util.*;
import java.util.stream.Stream;

public class FriendsFilterFunction extends AbstractRichFunction implements FilterFunction<PersonSimilarity> {

	private static final long serialVersionUID = 1L;

	private final String knowsRelationFile;

	private Map<Long, Set<Long>> knowsRelation;

	public FriendsFilterFunction(String knowsRelationFile) {
		this.knowsRelationFile = knowsRelationFile;
		this.knowsRelation = new HashMap<>();
	}

	Map<Long, Set<Long>> getKnowsRelation() {
		return this.knowsRelation;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		Stream<Tuple> knownFriends = StaticDataParser.parseCsvFile(this.knowsRelationFile);
		knownFriends.forEach(knowsTuple -> {
			Long person1 = Long.valueOf(knowsTuple.getField(0));
			Long person2 = Long.valueOf(knowsTuple.getField(1));

			Long key;
			Long value;
			if (person1 < person2) {
				key = person1;
				value = person2;
			} else {
				key = person2;
				value = person1;
			}

			Set<Long> set = this.knowsRelation.getOrDefault(key, new HashSet<>());
			set.add(value);
			this.knowsRelation.put(key, set);
		});
	}

	@Override
	public boolean filter(PersonSimilarity personSimilarity) {
		Long person1 = personSimilarity.person1Id();
		Long person2 = personSimilarity.person2Id();

		// filter out similarities computed against the same person
		if (person1.equals(person2)) {
			return false;
		}

		Long key;
		Long value;
		if (person1 < person2) {
			key = person1;
			value = person2;
		} else {
			key = person2;
			value = person1;
		}

		// filter out similarities to known friends
		return !(this.knowsRelation.containsKey(key) && this.knowsRelation.get(key).contains(value));
	}
}
