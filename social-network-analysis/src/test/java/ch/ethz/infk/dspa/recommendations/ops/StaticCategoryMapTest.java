package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity.PersonActivityType;
import ch.ethz.infk.dspa.recommendations.dto.StaticCategoryMap;

public class StaticCategoryMapTest {

	@Test
	void testStaticCategoryMap() throws Exception {

		final String staticFilePath = "src/test/java/resources/recommendations/relations/";
		final String staticPersonSpeaksLanguage = staticFilePath + "person_speaks_language.csv";
		final String staticPersonHasInterest = staticFilePath + "person_hasInterest_tag.csv";
		final String staticPersonIsLocatedIn = staticFilePath + "person_isLocatedIn_place.csv";
		final String staticPersonWorkAt = staticFilePath + "person_workAt_organisation.csv";
		final String staticPersonStudyAt = staticFilePath + "person_studyAt_organisation.csv";

		Set<Tuple2<String, Integer>> expectedCategories = new HashSet<>(Arrays.asList(

				// new contents from static files
				Tuple2.of("tag_0", 1),
				Tuple2.of("tag_1", 1),
				Tuple2.of("place_0", 1),
				Tuple2.of("lan_de", 1),
				Tuple2.of("lan_en", 1),
				Tuple2.of("org_1", 2),
				Tuple2.of("tag_2", 1)));

		StaticCategoryMap staticCategoryMap = new StaticCategoryMap()
				.withPersonInterestRelation(staticPersonHasInterest)
				.withPersonLocationRelation(staticPersonIsLocatedIn)
				.withPersonSpeaksRelation(staticPersonSpeaksLanguage)
				.withPersonStudyWorkAtRelations(staticPersonStudyAt, staticPersonWorkAt);

		// Check category map
		Set<Tuple2<String, Integer>> actualCategories = staticCategoryMap.getCategoryMap().get(0L).entrySet().stream()
				.map(entry -> Tuple2.of(entry.getKey(), entry.getValue()))
				.collect(Collectors.toSet());
		assertEquals(expectedCategories, actualCategories);

		// check person activities
		List<PersonActivity> activities = staticCategoryMap.getPersonActivities();
		assertEquals(activities.size(), 4, "activities size does not match");

		PersonActivity expectedActivity = new PersonActivity(0L, null, PersonActivityType.STATIC);
		expectedActivity.setCategoryMap(staticCategoryMap.getCategoryMap().get(0L));

		PersonActivity actualActivity = activities.stream().filter(a -> a.getPersonId() == 0).findAny().get();

		assertEquals(expectedActivity, actualActivity, "unexpected conversion to person activity");

	}
}
