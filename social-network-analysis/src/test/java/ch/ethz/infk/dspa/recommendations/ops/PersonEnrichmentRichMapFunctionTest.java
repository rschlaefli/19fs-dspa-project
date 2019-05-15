package ch.ethz.infk.dspa.recommendations.ops;

import ch.ethz.infk.dspa.recommendations.dto.Category;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PersonEnrichmentRichMapFunctionTest {

	private PersonEnrichmentRichMapFunction mapFunc;

	@BeforeEach
	void setup() throws Exception {
		final String staticFilePath = "src/test/java/resources/recommendations/relations/";
		final String staticPersonSpeaksLanguage = staticFilePath + "person_speaks_language.csv";
		final String staticPersonHasInterest = staticFilePath + "person_hasInterest_tag.csv";
		final String staticPersonIsLocatedIn = staticFilePath + "person_isLocatedIn_place.csv";
		final String staticPersonWorkAt = staticFilePath + "person_workAt_organisation.csv";
		final String staticPersonStudyAt = staticFilePath + "person_studyAt_organisation.csv";

		mapFunc = new PersonEnrichmentRichMapFunction(staticPersonSpeaksLanguage, staticPersonHasInterest,
				staticPersonIsLocatedIn, staticPersonWorkAt, staticPersonStudyAt);

		mapFunc.open(new Configuration());
	}

	@Test
	void testPersonEnrichmentRichMapFunction_correctlyEnrichesP0() throws Exception {
		PersonActivity personActivity = new PersonActivity(0L, 2L, null);
		personActivity.countCategory(Category.tag(5L));
		personActivity.countCategory(Category.tag(2L));
		personActivity.countCategory(Category.forum(3L));

		Set<Tuple2<String, Integer>> expectedCategories = new HashSet<>(Arrays.asList(
				// existing map contents that should not be touched
				Tuple2.of("forum_3", 1),
				Tuple2.of("tag_5", 1),

				// new contents from static files
				Tuple2.of("tag_0", 1),
				Tuple2.of("tag_1", 1),
				Tuple2.of("place_0", 1),
				Tuple2.of("lan_de", 1),
				Tuple2.of("lan_en", 1),
				Tuple2.of("org_1", 2),

				// contents of existing and static combined
				Tuple2.of("tag_2", 2)));

		PersonActivity mappedActivity = mapFunc.map(personActivity);

		Set<Tuple2<String, Integer>> actualCategories = mappedActivity
				.categoryMap().entrySet().stream()
				.map(entry -> Tuple2.of(entry.getKey(), entry.getValue()))
				.collect(Collectors.toSet());

		assertEquals(expectedCategories, actualCategories);
	}
}
