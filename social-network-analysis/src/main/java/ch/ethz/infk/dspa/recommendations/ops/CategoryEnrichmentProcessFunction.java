package ch.ethz.infk.dspa.recommendations.ops;

import java.util.*;
import java.util.stream.Collectors;

import ch.ethz.infk.dspa.helper.tuple.NTuple2;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infk.dspa.helper.StaticDataParser;
import ch.ethz.infk.dspa.recommendations.dto.Category;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;

public class CategoryEnrichmentProcessFunction extends KeyedProcessFunction<Long, PersonActivity, PersonActivity> {

	private static final long serialVersionUID = 1L;

	private final String forumTagsRelationFile;
	private final String placeRelationFile;

	private Map<Long, Set<Long>> forumTags;
	private Map<Long, Long> countryContinentMapping;

	private MapState<Long, Set<PersonActivity>> bufferedPersonActivities;
	private MapState<String, Integer> postCategories;
	private ValueState<Long> forumId;

	public CategoryEnrichmentProcessFunction(String forumTagsRelationFile, String placeRelationFile) {
		this.forumTagsRelationFile = forumTagsRelationFile;
		this.placeRelationFile = placeRelationFile;

		this.forumTags = new HashMap<>();
		this.countryContinentMapping = new HashMap<>();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		StaticDataParser.parseCsvFile(this.forumTagsRelationFile, Arrays.asList("Forum.id", "Tag.id"))
				.map(tuple -> (NTuple2<String, String>) tuple)
				.forEach(tuple -> {
					Long forumId = Long.valueOf(tuple.get("Forum.id"));
					Long tagId = Long.valueOf(tuple.get("Tag.id"));

					Set<Long> tagSet = this.forumTags.getOrDefault(forumId, new HashSet<>());
					tagSet.add(tagId);
					this.forumTags.put(forumId, tagSet);
				});

		StaticDataParser
				.parseCsvFile(this.placeRelationFile, Arrays.asList("Place.id", "Place.id.2"))
				.map(tuple -> (NTuple2<String, String>) tuple)
				.filter(tuple -> Long.valueOf(tuple.get("Place.id")) >= 0
						&& Long.valueOf(tuple.get("Place.id.2")) <= 110)
				.forEach(tuple -> {
					Long countryId = Long.valueOf(tuple.getField(0));
					Long continentId = Long.valueOf(tuple.getField(1));
					this.countryContinentMapping.put(countryId, continentId);
				});

		this.forumId = getRuntimeContext().getState(new ValueStateDescriptor<>(
				"PostToForumIdMapping",
				BasicTypeInfo.LONG_TYPE_INFO));

		this.postCategories = getRuntimeContext()
				.getMapState(new MapStateDescriptor<String, Integer>("PostCategoryState",
						BasicTypeInfo.STRING_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO));

		this.bufferedPersonActivities = getRuntimeContext()
				.getMapState(new MapStateDescriptor<>("BufferedPersonActivities",
						BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<Set<PersonActivity>>() {
						})));
	}

	@Override
	public void processElement(PersonActivity in, Context ctx, Collector<PersonActivity> out) throws Exception {
		Long forumId = in.extractIdFromKeySet("forum");

		if (forumId != null) {
			// if the activity contains the forum id, we are processing a post
			// if we are processing a post, store its forum id and all of its categories
			this.forumId.update(forumId);
			this.postCategories.putAll(filterCategoryMap(in.categoryMap()));

			// we can go on to enrich the current activity based on this
			enrichForumTags(in, forumId);
			enrichContinentId(in);

			// return the enriched activity
			out.collect(in);
		} else if (this.forumId.value() != null) {
			// if we already have a forum id (and category map), we can directly enrich and continue
			enrichPostInformation(in);

			// if we know the forum id, we either do or already have processed the relevant post
			// we can go on to enrich the current activity based on this
			enrichForumTags(in, this.forumId.value());
			enrichContinentId(in);

			// return the enriched activity
			out.collect(in);
		} else {
			// if the forum id is not already available, buffer the activity until watermark passes
			Set<PersonActivity> updatedSet = ObjectUtils
					.defaultIfNull(this.bufferedPersonActivities.get(ctx.timestamp()), new HashSet<>());
			updatedSet.add(in);
			this.bufferedPersonActivities.put(ctx.timestamp(), updatedSet);

			ctx.timerService().registerEventTimeTimer(ctx.timestamp());
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<PersonActivity> out) throws Exception {
		Set<PersonActivity> bufferedActivities = this.bufferedPersonActivities.get(timestamp);
		this.bufferedPersonActivities.remove(timestamp);

		for (PersonActivity bufferedActivity : bufferedActivities) {
			enrichPostInformation(bufferedActivity);
			enrichForumTags(bufferedActivity, this.forumId.value());
			enrichContinentId(bufferedActivity);
			out.collect(bufferedActivity);
		}
	}

	private void enrichPostInformation(PersonActivity in) throws Exception {
		// if the forum id is already available, process comments and likes immediately
		// the respective post must have been processed already
		if (this.forumId.value() != null) {
			in.countCategory(Category.forum(this.forumId.value()));
		}

		// enrich the category maps of comments and likes with post tags
		HashMap<String, Integer> restoredCategoryMap = new HashMap<>();
		for (Map.Entry<String, Integer> entry : this.postCategories.entries()) {
			restoredCategoryMap.put(entry.getKey(), entry.getValue());
		}
		in.mergeCategoryMap(restoredCategoryMap);
	}

	private void enrichForumTags(PersonActivity in, Long forumId) {
		// include tags of the forum in PersonActivity categories
		if (forumId != null && this.forumTags.containsKey(forumId)) {
			this.forumTags.get(forumId).forEach(tagId -> in.countCategory(Category.tag(tagId)));
		}
	}

	private void enrichContinentId(PersonActivity in) {
		// place is always extracted from the activity
		Long placeId = in.extractIdFromKeySet("place");

		// count the continent in the category map of the activity based on its country
		if (placeId != null && this.countryContinentMapping.containsKey(placeId)) {
			in.countCategory(Category.place(this.countryContinentMapping.get(placeId)));
		}
	}

	private Map<String, Integer> filterCategoryMap(Map<String, Integer> categoryMap) {
		return categoryMap.entrySet()
				.stream()
				// extract only categories that are actually related to tags
				// TODO: should we enrich anything other than tags from the category map?
				.filter(entry -> entry.getKey().startsWith("tag_"))
				// reduce to a list of map entries
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}
}
