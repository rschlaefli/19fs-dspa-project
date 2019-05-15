package ch.ethz.infk.dspa.recommendations.ops;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.google.common.collect.Streams;

import ch.ethz.infk.dspa.helper.StaticDataParser;
import ch.ethz.infk.dspa.helper.tuple.NTuple2;
import ch.ethz.infk.dspa.recommendations.dto.Category;
import ch.ethz.infk.dspa.recommendations.dto.Category.CategoryType;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity.PersonActivityType;

public class CategoryEnrichmentProcessFunction extends KeyedProcessFunction<Long, PersonActivity, PersonActivity> {

	private static final long serialVersionUID = 1L;

	private final String forumTagsRelationFile;
	private final String placeRelationFile;
	private final String tagHasTypeTagClassRelationFile;
	private final String tagclassIsSubclassOfTagClassRelationFile;

	private Map<String, List<String>> forumTagRelation;
	private Map<String, String> countryContinentRelation;
	private Map<String, List<String>> tagClassRelation;

	private MapState<Long, Set<PersonActivity>> bufferedPersonActivities;
	private MapState<String, Integer> inheritablePostCategoryMapState;

	public CategoryEnrichmentProcessFunction(String forumTagsRelationFile, String placeRelationFile,
			String tagHasTypeTagClassRelationFile, String tagclassIsSubclassOfTagClassRelationFile) {
		this.forumTagsRelationFile = forumTagsRelationFile;
		this.placeRelationFile = placeRelationFile;
		this.tagHasTypeTagClassRelationFile = tagHasTypeTagClassRelationFile;
		this.tagclassIsSubclassOfTagClassRelationFile = tagclassIsSubclassOfTagClassRelationFile;

		this.forumTagRelation = new HashMap<>();
		this.countryContinentRelation = new HashMap<>();
		this.tagClassRelation = new HashMap<>();

	}

	@Override
	public void processElement(PersonActivity in, Context ctx, Collector<PersonActivity> out) throws Exception {

		if (in.getType() == PersonActivityType.POST) {
			// PersonActivity is Post
			in = enrichPersonActivity(in, new HashMap<>());

			// store the categories which are inherited to interactions (comments, likes) of this post
			Map<String, Integer> inheritablePostCategoryMap = extractInheritableCategories(in);
			inheritablePostCategoryMapState.putAll(inheritablePostCategoryMap);

			// return the enriched post activity
			out.collect(in);

		} else if (inheritablePostCategoryMapState.iterator().hasNext()) {
			// PersonActivity is Comment / Like and corresponding Post was already processed
			Map<String, Integer> inheritedCategoriesFromPost = getInheritedCategoriesFromPost();
			in = enrichPersonActivity(in, inheritedCategoriesFromPost);

			// return the enriched activity
			out.collect(in);

		} else {
			// PersonActivity is Comment / Like -> Needs to be buffered
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

		Map<String, Integer> inheritedCategoriesFromPost = getInheritedCategoriesFromPost();

		for (PersonActivity bufferedActivity : bufferedActivities) {
			bufferedActivity = enrichPersonActivity(bufferedActivity, inheritedCategoriesFromPost);
			out.collect(bufferedActivity);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {

		this.forumTagRelation = buildForumTagRelation(this.forumTagsRelationFile);
		this.countryContinentRelation = buildContinentMappingRelation(this.placeRelationFile);
		this.tagClassRelation = buildTagClassRelation(this.tagHasTypeTagClassRelationFile,
				this.tagclassIsSubclassOfTagClassRelationFile);

		// TODO maybe add expiration?
		this.inheritablePostCategoryMapState = getRuntimeContext()
				.getMapState(new MapStateDescriptor<String, Integer>("PostCategoryState",
						BasicTypeInfo.STRING_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO));

		this.bufferedPersonActivities = getRuntimeContext()
				.getMapState(new MapStateDescriptor<>("BufferedPersonActivities",
						BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<Set<PersonActivity>>() {
						})));
	}

	/**
	 * Enriches the PersonActivity depending on whether it's a Post, Comment or a Like
	 */
	public PersonActivity enrichPersonActivity(PersonActivity activity,
			Map<String, Integer> inheritedCategoriesFromPost) throws Exception {

		switch (activity.getType()) {
		case POST:
			// enrich tags from forum
			activity.mergeCategoryMap(getForumTagsEnrichment(activity));

			// enrich tagClasses from tags (including forum tags)
			activity.mergeCategoryMap(getTagClassesEnrichment(activity));

			// enrich continents from places
			activity.mergeCategoryMap(getContinentEnrichment(activity));

			return activity;

		case COMMENT:
			// enrich continents from places
			activity.mergeCategoryMap(getContinentEnrichment(activity));

			// add the inherited categories from the post
			activity.mergeCategoryMap(inheritedCategoriesFromPost);

			return activity;

		case LIKE:
			// add the inherited categories from the post
			activity.mergeCategoryMap(inheritedCategoriesFromPost);

			return activity;

		default:
			throw new IllegalArgumentException("Unknown PersonActivity Type");
		}

	}

	/**
	 * Takes the category map of a Post PersonActivity and extracts all categories which should be inherited by
	 * comments/likes of this post
	 */
	public Map<String, Integer> extractInheritableCategories(PersonActivity in) {

		if (in.getType() != PersonActivityType.POST) {
			throw new IllegalArgumentException("Can only inherit categories from Post");
		}

		Map<String, Integer> inheritableCategories = new HashMap<>();

		Map<String, Integer> forumMap = in.getCategories(CategoryType.FORUM);
		inheritableCategories.putAll(forumMap);

		Map<String, Integer> tagMap = in.getCategories(CategoryType.TAG);
		inheritableCategories.putAll(tagMap);

		return inheritableCategories;
	}

	/**
	 * Looks at all tags in the PersonActivity and looks up the tag classes
	 */
	private Map<String, Integer> getTagClassesEnrichment(PersonActivity in) {
		List<String> tags = in.getCategoryKeys(CategoryType.FORUM);

		Map<String, Integer> tagClasses = tags.stream()
				.map(tag -> tagClassRelation.get(tag))
				.filter(x -> x != null)
				.flatMap(List::stream)
				.collect(Collectors.groupingBy(Function.identity(), Collectors.reducing(0, e -> 1, Integer::sum)));

		return tagClasses;
	}

	/**
	 * Looks at all forum ids (usually only one) in the PersonActivity and looks up the forum tags in the
	 * forumTagRelation
	 */
	private Map<String, Integer> getForumTagsEnrichment(PersonActivity in) {
		List<String> forums = in.getCategoryKeys(CategoryType.FORUM);
		Map<String, Integer> forumTags = forums.stream()
				.map(forumId -> forumTagRelation.get(forumId))
				.filter(x -> x != null)
				.flatMap(List::stream)
				.collect(Collectors.groupingBy(Function.identity(), Collectors.reducing(0, e -> 1, Integer::sum)));
		return forumTags;
	}

	/**
	 * Looks at all place ids in the PersonActivity and looks up the continent in the countryContinentRelation
	 */
	private Map<String, Integer> getContinentEnrichment(PersonActivity in) {
		List<String> places = in.getCategoryKeys(CategoryType.PLACE);
		Map<String, Integer> continents = places.stream().map(place -> countryContinentRelation.get(place))
				.filter(x -> x != null)
				.collect(Collectors.groupingBy(Function.identity(), Collectors.reducing(0, e -> 1, Integer::sum)));
		return continents;
	}

	/**
	 * Transforms the MapState inheritablePostCategoryMapStat to a Map
	 */
	private Map<String, Integer> getInheritedCategoriesFromPost() throws Exception {
		Map<String, Integer> inheritedCategoriesFromPost = Streams
				.stream(inheritablePostCategoryMapState.iterator())
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
		return inheritedCategoriesFromPost;
	}

	/**
	 * Builds the forum tag relation from the given file. As keys it uses the forumId in the Category String format
	 * (e.g. forum_1) and all tags in the value also have the Category String format (e.g. tag_10)
	 */
	public Map<String, List<String>> buildForumTagRelation(String forumTagsRelationFile) throws IOException {

		Map<String, List<String>> forumTags = new HashMap<>();
		StaticDataParser.parseCsvFile(forumTagsRelationFile, Arrays.asList("Forum.id", "Tag.id"))
				.map(tuple -> (NTuple2<String, String>) tuple)
				.forEach(tuple -> {

					String forum = Category.forum(Long.valueOf(tuple.get("Forum.id")));
					String tag = Category.tag(Long.valueOf(tuple.get("Tag.id")));

					List<String> tags = forumTags.getOrDefault(forum, new ArrayList<>());
					tags.add(tag);
					forumTags.put(forum, tags);
				});

		return forumTags;
	}

	/**
	 * Builds relation with a mapping between a tag (Category String format) and all related tagClasses and their parent
	 * tagClasses
	 */
	public Map<String, List<String>> buildTagClassRelation(String tagHasTypeTagClassRelationFile,
			String tagclassIsSubclassOfTagClassRelationFile) throws IOException {

		Map<String, List<String>> tagClasses = new HashMap<>();
		StaticDataParser
				.parseCsvFile(tagclassIsSubclassOfTagClassRelationFile, Arrays.asList("TagClass.id", "TagClass.id.2"))
				.map(tuple -> (NTuple2<String, String>) tuple)
				.forEach(tuple -> {
					String tagSubclass = Category.tagclass(Long.valueOf(tuple.get("TagClass.id")));
					String tagParentclass = Category.tagclass(Long.valueOf(tuple.get("TagClass.id.2")));

					List<String> parentClasses = tagClasses.getOrDefault(tagSubclass, new ArrayList<>());
					parentClasses.add(tagParentclass);
					tagClasses.put(tagSubclass, parentClasses);
				});
		Map<String, List<String>> tagTagClassMapping = new HashMap<>();
		StaticDataParser.parseCsvFile(tagHasTypeTagClassRelationFile, Arrays.asList("Tag.id", "TagClass.id"))
				.map(tuple -> (NTuple2<String, String>) tuple)
				.forEach(tuple -> {
					String tag = Category.tag(Long.valueOf(tuple.get("Tag.id")));
					String tagClass = Category.tagclass(Long.valueOf(tuple.get("TagClass.id")));

					List<String> parentClasses = tagClasses.getOrDefault(tagClass, new ArrayList<>());

					List<String> classes = tagTagClassMapping.getOrDefault(tag, new ArrayList<>());
					classes.addAll(parentClasses);

					tagTagClassMapping.put(tag, classes);
				});

		return tagTagClassMapping;
	}

	/**
	 * Builds the continent mapping relation from the given file. Both key and value use the Category String format
	 * (e.g. place_1)
	 */
	public Map<String, String> buildContinentMappingRelation(String placeRelationFile) throws IOException {
		Map<String, String> countryContinentMapping = new HashMap<>();
		StaticDataParser
				.parseCsvFile(placeRelationFile, Arrays.asList("Place.id", "Place.id.2"))
				.map(tuple -> (NTuple2<String, String>) tuple)
				.filter(tuple -> Long.valueOf(tuple.get("Place.id")) >= 0
						&& Long.valueOf(tuple.get("Place.id.2")) <= 110)
				.forEach(tuple -> {
					String countryId = Category.place(Long.valueOf(tuple.getField(0)));
					String continentId = Category.place(Long.valueOf(tuple.getField(1)));
					countryContinentMapping.put(countryId, continentId);
				});

		return countryContinentMapping;
	}
}
