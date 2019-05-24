package ch.ethz.infk.dspa.recommendations.ops;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import ch.ethz.infk.dspa.helper.StaticDataParser;
import ch.ethz.infk.dspa.helper.tuple.NTuple2;
import ch.ethz.infk.dspa.helper.tuple.NTuple8;
import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation;

public class PersonNameMapFunction extends RichMapFunction<FriendsRecommendation, FriendsRecommendation> {

	private static final long serialVersionUID = 1L;

	private String personRelationFile;

	private Map<Long, String> userNameRelation;

	public PersonNameMapFunction(String personRelationFile) {
		this.personRelationFile = personRelationFile;
	}

	@Override
	public FriendsRecommendation map(FriendsRecommendation element) throws Exception {
		element.setPersonName(userNameRelation.get(element.getPersonId()));

		element.getSimilarities().forEach(x -> x.setPersonName(userNameRelation.get(x.getPersonId())));

		return element;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.userNameRelation = buildUserNameRelation(this.personRelationFile);
	}

	public static Map<Long, String> buildUserNameRelation(String personRelationFile) throws IOException {
		@SuppressWarnings("unchecked")
		Map<Long, String> userNameRelation = StaticDataParser
				.parseCsvFile(personRelationFile,
						Arrays.asList("id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP",
								"browserUsed"))
				.map(tuple -> (NTuple8<String, String, String, String, String, String, String, String>) tuple)
				.map(tuple -> NTuple2.of("personId", Long.valueOf(tuple.get("id")), "name",
						tuple.get("firstName") + " " + tuple.get("lastName")))
				.collect(Collectors.toMap(t -> t.get("personId"), t -> t.get("name")));

		return userNameRelation;
	}

}
