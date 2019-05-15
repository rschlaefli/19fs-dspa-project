package ch.ethz.infk.dspa.recommendations.ops;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.StaticCategoryMap;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.*;

public class PersonEnrichmentRichMapFunction extends RichMapFunction<PersonActivity, PersonActivity> {

	private final String personSpeaksRelationFile;
	private final String personInterestRelationFile;
	private final String personLocationRelationFile;
	private final String personWorkplaceRelationFile;
	private final String personStudyRelationFile;

	private Map<Long, Map<String, Integer>> staticPersonCategories;

	public PersonEnrichmentRichMapFunction(String personSpeaksRelationFile, String personInterestRelationFile,
			String personLocationRelationFile, String personWorkplaceRelationFile, String personStudyRelationFile) {
		this.personSpeaksRelationFile = personSpeaksRelationFile;
		this.personInterestRelationFile = personInterestRelationFile;
		this.personLocationRelationFile = personLocationRelationFile;
		this.personWorkplaceRelationFile = personWorkplaceRelationFile;
		this.personStudyRelationFile = personStudyRelationFile;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		StaticCategoryMap staticCategoryMap = new StaticCategoryMap()
				.withPersonInterestRelation(personInterestRelationFile)
				.withPersonLocationRelation(personLocationRelationFile)
				.withPersonSpeaksRelation(personSpeaksRelationFile)
				.withPersonStudyWorkAtRelations(personStudyRelationFile, personWorkplaceRelationFile);

		this.staticPersonCategories = staticCategoryMap.getCategoryMap();
	}

	@Override
	public PersonActivity map(PersonActivity personActivity) throws Exception {
		// if we have static information for the current person id, enrich it
		if (this.staticPersonCategories.containsKey(personActivity.personId())) {
			Map<String, Integer> categoryMap = this.staticPersonCategories.get(personActivity.personId());
			personActivity.mergeCategoryMap(categoryMap);
		}

		return personActivity;
	}
}
