package ch.ethz.infk.dspa.recommendations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import ch.ethz.infk.dspa.AbstractAnalyticsTaskIT;
import ch.ethz.infk.dspa.ResultWindow;
import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.helper.StaticDataParser;
import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation;
import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation.SimilarityTuple;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity.PersonActivityType;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarityComparator;
import ch.ethz.infk.dspa.recommendations.dto.StaticCategoryMap;
import ch.ethz.infk.dspa.recommendations.ops.CategoryEnrichmentProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonNameMapFunction;
import ch.ethz.infk.dspa.stream.helper.TestSink;

public class RecommendationsAnalyticsTaskIT extends AbstractAnalyticsTaskIT<FriendsRecommendation> {

	private Set<Long> recommendationPersonIds;
	private boolean outputCategoryMap;

	private CategoryEnrichmentProcessFunction enrichment;
	private String forumTagsRelationFile = getStaticFilePath() + "forum_hasTag_tag.csv";
	private String placeRelationFile = getStaticFilePath() + "place_isPartOf_place.csv";
	private String tagHasTypeTagClassRelationFile = getStaticFilePath() + "tag_hasType_tagclass.csv";
	private String tagclassIsSubclassOfTagClassRelationFile = getStaticFilePath()
			+ "tagclass_isSubclassOf_tagclass.csv";

	private Map<Long, String> personNameRelation;
	private Set<String> knowsRelation;
	private String knowsRelationFile = getStaticFilePath() + "person_knows_person.csv";

	private Map<Long, Map<String, Integer>> inheritedCategoryMap = new HashMap<>();
	private List<PersonActivity> staticPersonActivities;
	private String personInterestRelationFile = getStaticFilePath() + "person_hasInterest_tag.csv";
	private String personLocationRelationFile = getStaticFilePath() + "person_isLocatedIn_place.csv";
	private String personSpeaksRelationFile = getStaticFilePath() + "person_speaks_language.csv";
	private String personStudyRelationFile = getStaticFilePath() + "person_studyAt_organisation.csv";
	private String personWorkplaceRelationFile = getStaticFilePath() + "person_workAt_organisation.csv";
	private String staticPersonFile = getStaticFilePath() + "person.csv";

	@Override
	public void beforeEachTaskSpecific(List<Post> allPosts, List<Comment> allComments, List<Like> allLikes)
			throws Exception {

		outputCategoryMap = getConfig().getBoolean("tasks.recommendations.outputCategoryMap");

		recommendationPersonIds = new HashSet<>(
				Arrays.asList(507L, 732L, 576L, 868L, 789L, 833L, 842L, 676L, 929L, 305L));

		enrichment = new CategoryEnrichmentProcessFunction(forumTagsRelationFile,
				placeRelationFile, tagHasTypeTagClassRelationFile, tagclassIsSubclassOfTagClassRelationFile);
		enrichment.buildForumTagRelation();
		enrichment.buildContinentMappingRelation();
		enrichment.buildTagClassRelation();

		knowsRelation = StaticDataParser.parseCsvFile(this.knowsRelationFile).map(tuple -> {
			Long person1 = Long.valueOf(tuple.getField(0));
			Long person2 = Long.valueOf(tuple.getField(1));

			return Math.min(person1, person2) + "_"
					+ Math.max(person1, person2);
		}).collect(Collectors.toSet());

		StaticCategoryMap staticCategoryMap = new StaticCategoryMap()
				.withPersonInterestRelation(personInterestRelationFile)
				.withPersonLocationRelation(personLocationRelationFile)
				.withPersonSpeaksRelation(personSpeaksRelationFile)
				.withPersonStudyWorkAtRelations(personStudyRelationFile, personWorkplaceRelationFile);

		this.staticPersonActivities = staticCategoryMap.getPersonActivities();

		// build inheritedCategoryMap
		this.inheritedCategoryMap = new HashMap<>();
		for (Post post : allPosts) {
			Map<String, Integer> inheritableCategories = enrichment.extractInheritableCategories(
					enrichment.enrichPersonActivity(PersonActivity.of(post), new HashMap<>()));
			this.inheritedCategoryMap.put(post.getId(), inheritableCategories);
		}

		this.personNameRelation = PersonNameMapFunction.buildUserNameRelation(this.staticPersonFile);

	}

	@Override
	public List<ResultWindow<FriendsRecommendation>> produceActualResults(DataStream<Post> posts,
			DataStream<Comment> comments, DataStream<Like> likes) throws Exception {

		Set<Long> recommendationPersonIds = new HashSet<>(
				Arrays.asList(507L, 732L, 576L, 868L, 789L, 833L, 842L, 676L, 929L, 305L));

		RecommendationsAnalyticsTask analyticsTask = (RecommendationsAnalyticsTask) new RecommendationsAnalyticsTask()
				.withRecommendationPersonIds(recommendationPersonIds)
				.withPropertiesConfiguration(getConfig())
				.withStreamingEnvironment(getEnv())
				.withStaticFilePath(getStaticFilePath())
				.withMaxDelay(getMaxOutOfOrderness())
				.withInputStreams(posts, comments, likes)
				.initialize()
				.build()
				.withSink(new TestSink<>());

		analyticsTask.start();

		return TestSink.getResultsInResultWindow(FriendsRecommendation.class,
				SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)), x -> true);

	}

	@Override
	public List<FriendsRecommendation> buildExpectedResultsOfWindow(WindowAssigner<Object, TimeWindow> assigner,
			TimeWindow timeWindow, List<Post> posts, List<Comment> comments, List<Like> likes) throws Exception {

		// build person activities from events in window
		List<PersonActivity> personActivities = getStreamPersonActivities(posts, comments, likes);

		// combine them with the static person activities
		personActivities.addAll(this.staticPersonActivities);

		// reduce them by personId
		List<PersonActivity> reducedPersonActivities = reducePersonActivities(personActivities);

		// filter persons for which recommendations should be made
		List<PersonActivity> selectedPersonActivities = reducedPersonActivities.stream()
				.filter(x -> this.recommendationPersonIds.contains(x.getPersonId()))
				.collect(Collectors.toList());

		List<FriendsRecommendation> recommendations = new ArrayList<>();

		// calculate all similarities between selected and all other expect same and friends
		for (PersonActivity p1 : selectedPersonActivities) {

			List<PersonSimilarity> personSimilarities = new ArrayList<>();
			for (PersonActivity p2 : reducedPersonActivities) {

				String knowsQuery = Math.min(p1.getPersonId(), p2.getPersonId()) + "_"
						+ Math.max(p1.getPersonId(), p2.getPersonId());

				// ignore similarity between same person and filter out friends
				if (p1.getPersonId() != p2.getPersonId() && !knowsRelation.contains(knowsQuery)) {
					PersonSimilarity similarity = PersonSimilarity.dotProduct(p1, p2);

					if (outputCategoryMap) {
						similarity.setCategoryMap1(p1.getCategoryMap());
						similarity.setCategoryMap2(p2.getCategoryMap());
					}

					personSimilarities.add(similarity);
				}
			}

			// select 5 largest similarities
			List<SimilarityTuple> top5Similarities = personSimilarities.stream()
					.sorted(new PersonSimilarityComparator().reversed())
					.limit(5)
					.map(x -> new SimilarityTuple(x.person2Id(), x.similarity(), x.getCategoryMap2(),
							personNameRelation.get(x.person2Id())))
					.collect(Collectors.toList());

			// create friends recommendation
			FriendsRecommendation recommendation = new FriendsRecommendation();
			recommendation.setPersonId(p1.getPersonId());
			recommendation.setPersonName(personNameRelation.get(p1.getPersonId()));
			recommendation.setSimilarities(top5Similarities);
			recommendation.setInactive(p1.onlyStatic());
			if (outputCategoryMap) {
				recommendation.setCategoryMap(p1.getCategoryMap());
			}
			recommendations.add(recommendation);
		}

		return recommendations;
	}

	private List<PersonActivity> getStreamPersonActivities(List<Post> posts, List<Comment> comments,
			List<Like> likes)
			throws Exception {

		// map events to PersonActivity
		List<PersonActivity> personActivities = new ArrayList<>();

		for (Post post : posts) {

			PersonActivity postPersonActivity = PersonActivity.of(post);

			// enrich post and extract categories which are inherited by comments and likes of the post
			postPersonActivity = enrichment.enrichPersonActivity(postPersonActivity, new HashMap<>());

			personActivities.add(postPersonActivity);
		}

		for (Comment comment : comments) {
			PersonActivity commentPersonActivity = PersonActivity.of(comment);
			// enrich comment
			assert (comment.getReplyToPostId() != null);
			commentPersonActivity = enrichment.enrichPersonActivity(commentPersonActivity,
					inheritedCategoryMap.get(comment.getReplyToPostId()));
			personActivities.add(commentPersonActivity);

		}
		for (Like like : likes) {
			PersonActivity likePersonActivity = PersonActivity.of(like);

			// enrich like
			likePersonActivity = enrichment.enrichPersonActivity(likePersonActivity,
					inheritedCategoryMap.get(like.getPostId()));

			personActivities.add(likePersonActivity);
		}
		return personActivities;
	}

	private List<PersonActivity> reducePersonActivities(List<PersonActivity> personActivities) {
		// reduce PersonActivity by personId
		Map<Long, List<PersonActivity>> activitiesPerPerson = personActivities.stream()
				.collect(Collectors.groupingBy(PersonActivity::getPersonId));

		List<PersonActivity> reducedPersonActivities = new ArrayList<>();
		for (Entry<Long, List<PersonActivity>> e : activitiesPerPerson.entrySet()) {
			Long personId = e.getKey();
			List<PersonActivity> activities = e.getValue();

			PersonActivity reducedActivity = new PersonActivity();
			reducedActivity.setPersonId(personId);

			Map<String, Integer> categoryMap = new HashMap<>();
			for (PersonActivity personActivity : activities) {
				personActivity.getCategoryMap()
						.forEach((category, count) -> categoryMap.merge(category, count, Integer::sum));
			}
			reducedActivity.setCategoryMap(categoryMap);

			boolean onlyStatic = activities.stream().noneMatch(x -> x.getType() != PersonActivityType.STATIC);
			if (onlyStatic) {
				reducedActivity.setType(PersonActivityType.STATIC);
			}

			reducedPersonActivities.add(reducedActivity);
		}

		return reducedPersonActivities;
	}

	@Override
	public List<WindowAssigner<Object, TimeWindow>> getWindowAssigners() {
		return Collections.singletonList(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)));
	}

	@Override
	public Time getMaxOutOfOrderness() {
		return Time.seconds(600);
	}

}
