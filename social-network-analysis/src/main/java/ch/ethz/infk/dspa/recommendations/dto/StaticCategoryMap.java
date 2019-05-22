package ch.ethz.infk.dspa.recommendations.dto;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.ethz.infk.dspa.helper.StaticDataParser;
import ch.ethz.infk.dspa.helper.tuple.NTuple2;
import ch.ethz.infk.dspa.helper.tuple.NTuple3;

public class StaticCategoryMap {

	private final Map<Long, Map<String, Integer>> categoryMap;

	public StaticCategoryMap() {
		this.categoryMap = new HashMap<>();
	}

	public Map<Long, Map<String, Integer>> getCategoryMap() {
		return this.categoryMap;
	}

	public List<PersonActivity> getPersonActivities() {
		return getCategoryMap().entrySet().stream()
				.map(e -> PersonActivity.ofStatic(e.getKey(), e.getValue()))
				.collect(Collectors.toList());
	}

	public StaticCategoryMap withPersonSpeaksRelation(String filePath) throws IOException {
		StaticDataParser.parseCsvFile(filePath, Arrays.asList("Person.id", "language"))
				.map(tuple -> (NTuple2<String, String>) tuple)
				.forEach(tuple -> {
					Long personId = Long.valueOf(tuple.get("Person.id"));
					String language = tuple.get("language");

					Map<String, Integer> categoryMap = this.categoryMap.getOrDefault(personId, new HashMap<>());
					categoryMap.merge(Category.language(language), 1, Integer::sum);
					this.categoryMap.put(personId, categoryMap);
				});

		return this;
	}

	public StaticCategoryMap withPersonInterestRelation(String filePath) throws IOException {
		StaticDataParser.parseCsvFile(filePath, Arrays.asList("Person.id", "Tag.id"))
				.map(tuple -> (NTuple2<String, String>) tuple)
				.forEach(tuple -> {
					Long personId = Long.valueOf(tuple.get("Person.id"));
					Long tagId = Long.valueOf(tuple.get("Tag.id"));

					Map<String, Integer> categoryMap = this.categoryMap.getOrDefault(personId, new HashMap<>());
					categoryMap.merge(Category.tag(tagId), 1, Integer::sum);
					this.categoryMap.put(personId, categoryMap);
				});

		return this;
	}

	public StaticCategoryMap withPersonLocationRelation(String filePath) throws IOException {
		StaticDataParser.parseCsvFile(filePath, Arrays.asList("Person.id", "Place.id"))
				.map(tuple -> (NTuple2<String, String>) tuple)
				.forEach(tuple -> {
					Long personId = Long.valueOf(tuple.get("Person.id"));
					Long placeId = Long.valueOf(tuple.get("Place.id"));

					Map<String, Integer> categoryMap = this.categoryMap.getOrDefault(personId, new HashMap<>());
					categoryMap.merge(Category.place(placeId), 1, Integer::sum);
					this.categoryMap.put(personId, categoryMap);
				});

		return this;
	}

	public StaticCategoryMap withPersonStudyWorkAtRelations(String filePathStudyAt, String filePathWorkAt)
			throws IOException {
		Stream<NTuple3<String, String, String>> workplaceStream = StaticDataParser
				.parseCsvFile(filePathWorkAt, Arrays.asList("Person.id", "Organisation.id", "Year"))
				.map(tuple -> (NTuple3<String, String, String>) tuple);
		Stream<NTuple3<String, String, String>> studyStream = StaticDataParser
				.parseCsvFile(filePathStudyAt, Arrays.asList("Person.id", "Organisation.id", "Year"))
				.map(tuple -> (NTuple3<String, String, String>) tuple);
		Stream.concat(workplaceStream, studyStream)
				.forEach(tuple -> {
					Long personId = Long.valueOf(tuple.get("Person.id"));
					Long organisationId = Long.valueOf(tuple.get("Organisation.id"));

					Map<String, Integer> categoryMap = this.categoryMap.getOrDefault(personId, new HashMap<>());
					categoryMap.merge(Category.organisation(organisationId), 1, Integer::sum);
					this.categoryMap.put(personId, categoryMap);
				});

		return this;
	}
}
