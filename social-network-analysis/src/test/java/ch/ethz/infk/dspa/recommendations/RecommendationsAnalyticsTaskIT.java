package ch.ethz.infk.dspa.recommendations;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.helper.Config;
import ch.ethz.infk.dspa.helper.StaticDataParser;
import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation;
import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation.SimilarityTuple;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import ch.ethz.infk.dspa.recommendations.ops.CategoryEnrichmentProcessFunction;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.AbstractTestDataGenerator.TestDataPair;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.LikeTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;

public class RecommendationsAnalyticsTaskIT extends AbstractTestBase {

	private Configuration config;
	private StreamExecutionEnvironment env;
	private DataStream<Post> postStream;
	private DataStream<Comment> commentStream;
	private DataStream<Like> likeStream;

	private List<Post> posts;
	private List<Comment> comments;
	private List<Like> likes;

	private CategoryEnrichmentProcessFunction enrichment;
	private String staticFilePath = "src/test/java/resources/relations/";
	private String forumTagsRelationFile = staticFilePath + "forum_hasTag_tag.csv";
	private String placeRelationFile = staticFilePath + "place_isPartOf_place";
	private String tagHasTypeTagClassRelationFile = staticFilePath + "tag_hasType_tagclass.csv";
	private String tagclassIsSubclassOfTagClassRelationFile = staticFilePath + "tagclass_isSubclassOf_tagclass.csv";

	private Set<String> knowsRelation;
	private String knowsRelationFile = staticFilePath + "person_knows_person.csv";

	private Map<Long, Map<String, Integer>> inheritedCategoryMap = new HashMap<>();

	@BeforeEach
	public void setup() throws Exception {
		final Time maxOutOfOrderness = Time.hours(1);

		config = Config.getConfig("src/main/java/ch/ethz/infk/dspa/config.properties");
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		PostTestDataGenerator postGenerator = new PostTestDataGenerator();
		postStream = postGenerator
				.generate(env, "src/test/java/resources/post_event_stream.csv", maxOutOfOrderness);
		posts = postGenerator.getTestData().stream().map(TestDataPair::getElement)
				.sorted(Comparator.comparing(Post::getCreationDate)).collect(Collectors.toList());

		CommentTestDataGenerator commentGenerator = new CommentTestDataGenerator();
		commentStream = commentGenerator
				.generate(env, "src/test/java/resources/comment_event_stream.csv", maxOutOfOrderness);
		comments = commentGenerator.getTestData().stream().map(TestDataPair::getElement)
				.sorted(Comparator.comparing(Comment::getCreationDate)).collect(Collectors.toList());

		LikeTestDataGenerator likeGenerator = new LikeTestDataGenerator();
		likeStream = likeGenerator
				.generate(env, "src/test/java/resources/likes_event_stream.csv", maxOutOfOrderness);
		likes = likeGenerator.getTestData().stream().map(TestDataPair::getElement)
				.sorted(Comparator.comparing(Like::getCreationDate)).collect(Collectors.toList());

		enrichment = new CategoryEnrichmentProcessFunction(forumTagsRelationFile,
				placeRelationFile, tagHasTypeTagClassRelationFile, tagclassIsSubclassOfTagClassRelationFile);

		knowsRelation = StaticDataParser.parseCsvFile(this.knowsRelationFile).map(tuple -> {
			Long person1 = Long.valueOf(tuple.getField(0));
			Long person2 = Long.valueOf(tuple.getField(1));

			return Math.min(person1, person2) + "_"
					+ Math.max(person1, person2);
		}).collect(Collectors.toSet());

	}

	@Test
	public void testRecommendationsConsumer() throws Exception {

		RecommendationsAnalyticsTask analyticsTask = (RecommendationsAnalyticsTask) new RecommendationsAnalyticsTask()
				.withPropertiesConfiguration(config)
				.withStreamingEnvironment(env)
				.withStaticFilePath(staticFilePath)
				.withMaxDelay(Time.seconds(600L))
				.withInputStreams(postStream, commentStream, likeStream)
				.initialize()
				.build()
				.withSink(new TestSink<>());

		analyticsTask.toStringStream().print();

		analyticsTask.start();

		// Map<windowStartTimestamp, Map<personId, FriendsRecommendation>>
		List<ResultWindow> expectedResults = buildExpectedResults(posts, comments, likes);

		System.out.println(expectedResults);

		// Map<event timestamp, List<JsonStr>>
		Map<Long, List<FriendsRecommendation>> rawResults = TestSink
				.getResultsInTumblingWindow(FriendsRecommendation.class, Time.hours(1));
		List<ResultWindow> results = processRawResults(rawResults);

		// sort the windows
		Collections.sort(expectedResults, Comparator.comparingLong(ResultWindow::getWindowStart));
		Collections.sort(results, Comparator.comparingLong(ResultWindow::getWindowStart));

		for (int i = 0; i < results.size(); i++) {
			// check correctness of window
			ResultWindow expectedResult = expectedResults.get(i);
			ResultWindow result = results.get(i);

			// check the correctness of the recommendation of each person
			for (Entry<Long, FriendsRecommendation> entry : result.recommendations.entrySet()) {
				long personId = entry.getKey();
				FriendsRecommendation recommendation = entry.getValue();
				FriendsRecommendation expRecommendation = expectedResult.getRecommendations().get(personId);

				String msg = String.format("Result window %d: Recommendation of Person %d does not match expected",
						entry.getKey(), personId);
				System.out.println(recommendation);
				assertEquals(expRecommendation, recommendation, msg);
			}
		}

	}

	public static class ResultWindow {
		private long windowStart;
		private Map<Long, FriendsRecommendation> recommendations;

		public long getWindowStart() {
			return windowStart;
		}

		public void setWindowStart(long windowStart) {
			this.windowStart = windowStart;
		}

		public Map<Long, FriendsRecommendation> getRecommendations() {
			return recommendations;
		}

		public void setRecommendations(Map<Long, FriendsRecommendation> recommendations) {
			this.recommendations = recommendations;
		}

		@Override
		public String toString() {
			return "ResultWindow [windowStart=" + windowStart + ", recommendations=" + recommendations + "]";
		}

	}

	/**
	 * Converts the rawResults (Map<windowStart, List<JsonString>) from the test sink into a List of Result Windows
	 */
	private List<ResultWindow> processRawResults(Map<Long, List<FriendsRecommendation>> rawResults) {

		List<ResultWindow> windows = new ArrayList<>();

		for (Entry<Long, List<FriendsRecommendation>> entry : rawResults.entrySet()) {

			Long windowStart = entry.getKey();
			List<FriendsRecommendation> jsonRecommendations = entry.getValue();

			Map<Long, FriendsRecommendation> recommendationMap = jsonRecommendations.stream()
					// collect as map keyed with the person for which the recommendations are
					.collect(Collectors.toMap(FriendsRecommendation::getPersonId, Function.identity()));

			ResultWindow window = new ResultWindow();
			window.setWindowStart(windowStart);
			window.setRecommendations(recommendationMap);

			windows.add(window);
		}

		return windows;
	}

	/**
	 * Takes as input the 3 event "streams", first builds windows with them, then each event in a window is transformed
	 * to a PersonActivity then all PersonActivity of a window are reduced by personId then PersonSimilarity between all
	 * pair of PersonActivity is calculated and finally the results are stored in a result window
	 * 
	 * @throws Exception
	 */
	private List<ResultWindow> buildExpectedResults(List<Post> posts, List<Comment> comments,
			List<Like> likes) throws Exception {

		List<ResultWindow> expectedResults = new ArrayList<>();

		for (Post post : posts) {
			PersonActivity postPersonActivity = PersonActivity.of(post);

			// enrich post and extract categories which are inherited by comments and likes of the post
			postPersonActivity = enrichment.enrichPersonActivity(postPersonActivity, new HashMap<>());
			Map<String, Integer> categories = enrichment.extractInheritableCategories(postPersonActivity);
			inheritedCategoryMap.put(post.getId(), categories);
		}

		// add comment to post mapping
		Map<Long, Long> commentToPostIdMap = new HashMap<>();
		for (Comment comment : comments) {

			Long postId = comment.getReplyToPostId();
			if (postId == null) {
				postId = commentToPostIdMap.get(comment.getReplyToCommentId());

			}
			assert (postId != null);
			commentToPostIdMap.put(comment.getId(), postId);
			comment.setReplyToPostId(postId);
		}

		long length = Time.hours(4).toMilliseconds();
		long slide = Time.hours(1).toMilliseconds();

		// with a window length of 4h and a slide of 1h, every event is part of (4/1) = 4 windows
		int numberOfWindowsPerEvent = (int) (length / slide);

		// assign events to windows (key=windowStart, value=list of all events in this window)
		Map<Long, List<Post>> postWindows = assignPostsToWindows(posts, slide, numberOfWindowsPerEvent);
		Map<Long, List<Comment>> commentWindows = assignCommentsToWindows(comments, slide, numberOfWindowsPerEvent);
		Map<Long, List<Like>> likeWindows = assignLikesToWindows(likes, slide, numberOfWindowsPerEvent);

		// merge all window starts
		Set<Long> windowStarts = new HashSet<>();
		windowStarts.addAll(postWindows.keySet());
		windowStarts.addAll(commentWindows.keySet());
		windowStarts.addAll(likeWindows.keySet());

		// sort window starts
		List<Long> sortedWindowStarts = new ArrayList<>(windowStarts);
		Collections.sort(sortedWindowStarts);

		// go through every window and calc expected result
		for (Long windowStart : sortedWindowStarts) {
			List<Post> postsInWindow = postWindows.getOrDefault(windowStart, Collections.emptyList());
			List<Comment> commentsInWindow = commentWindows.getOrDefault(windowStart, Collections.emptyList());
			List<Like> likesInWindow = likeWindows.getOrDefault(windowStart, Collections.emptyList());

			// from events in window build person activities in window
			List<PersonActivity> personActivitiesInWindow = buildPersonActivities(postsInWindow, commentsInWindow,
					likesInWindow);

			// from person activities in window build person similarities in window
			Map<Long, FriendsRecommendation> personSimilaritiesInWindow = calcPersonSimilarities(
					personActivitiesInWindow);

			// create result window and add to expected results
			ResultWindow window = new ResultWindow();
			window.setWindowStart(windowStart);
			window.setRecommendations(personSimilaritiesInWindow);
			expectedResults.add(window);

		}

		return expectedResults;

	}

	private Map<Long, List<Like>> assignLikesToWindows(List<Like> likes, long slide, int numberOfWindowsPerEvent) {
		Map<Long, List<Like>> likeWindows = new HashMap<>();
		for (Like like : likes) {
			long baseWindowStart = TimeWindow.getWindowStartWithOffset(like.getCreationDate().getMillis(), 0, slide);
			for (int i = 0; i < numberOfWindowsPerEvent; i++) {
				long windowStart = baseWindowStart + i * slide;
				List<Like> windowLikes = likeWindows.getOrDefault(windowStart, new ArrayList<Like>());
				windowLikes.add(like);
				likeWindows.put(windowStart, windowLikes);
			}
		}
		return likeWindows;
	}

	private Map<Long, List<Comment>> assignCommentsToWindows(List<Comment> comments, long slide,
			int numberOfWindowsPerEvent) {
		Map<Long, List<Comment>> commentWindows = new HashMap<>();
		for (Comment comment : comments) {
			long baseWindowStart = TimeWindow.getWindowStartWithOffset(comment.getCreationDate().getMillis(), 0, slide);
			for (int i = 0; i < numberOfWindowsPerEvent; i++) {
				long windowStart = baseWindowStart + i * slide;
				List<Comment> windowComments = commentWindows.getOrDefault(windowStart, new ArrayList<Comment>());
				windowComments.add(comment);
				commentWindows.put(windowStart, windowComments);
			}
		}
		return commentWindows;
	}

	private Map<Long, List<Post>> assignPostsToWindows(List<Post> posts, long slide, int numberOfWindowsPerEvent) {
		Map<Long, List<Post>> postWindows = new HashMap<>();
		for (Post post : posts) {
			long baseWindowStart = TimeWindow.getWindowStartWithOffset(post.getCreationDate().getMillis(), 0, slide);
			for (int i = 0; i < numberOfWindowsPerEvent; i++) {
				long windowStart = baseWindowStart + i * slide;
				List<Post> windowPosts = postWindows.getOrDefault(windowStart, new ArrayList<Post>());
				windowPosts.add(post);
				postWindows.put(windowStart, windowPosts);
			}
		}
		return postWindows;
	}

	private List<PersonActivity> buildPersonActivities(List<Post> postStream, List<Comment> commentStream,
			List<Like> likeStream) throws Exception {

		// map events to PersonActivity
		List<PersonActivity> personActivities = new ArrayList<>();

		for (Post post : postStream) {
			PersonActivity postPersonActivity = PersonActivity.of(post);

			// enrich post and extract categories which are inherited by comments and likes of the post
			postPersonActivity = enrichment.enrichPersonActivity(postPersonActivity, new HashMap<>());

			personActivities.add(postPersonActivity);
		}

		for (Comment comment : commentStream) {
			PersonActivity commentPersonActivity = PersonActivity.of(comment);
			// enrich comment
			commentPersonActivity = enrichment.enrichPersonActivity(commentPersonActivity,
					inheritedCategoryMap.get(comment.getReplyToPostId()));
			personActivities.add(commentPersonActivity);

		}
		for (Like like : likeStream) {
			PersonActivity likePersonActivity = PersonActivity.of(like);

			// enrich like
			likePersonActivity = enrichment.enrichPersonActivity(likePersonActivity,
					inheritedCategoryMap.get(like.getPostId()));

			personActivities.add(likePersonActivity);
		}

		// reduce PersonActivity by personId
		Map<Long, List<PersonActivity>> activitiesPerPerson = personActivities.stream()
				.collect(Collectors.groupingBy(PersonActivity::personId));

		List<PersonActivity> reducedPersonActivities = new ArrayList<>();
		for (Entry<Long, List<PersonActivity>> e : activitiesPerPerson.entrySet()) {
			Long personId = e.getKey();
			List<PersonActivity> activities = e.getValue();

			PersonActivity reducedActivity = new PersonActivity();
			reducedActivity.setPersonId(personId);

			activities.stream().forEach(a -> reducedActivity.mergeCategoryMap(a.categoryMap()));

			// TODO [nku] merge reducedActivity with static here

			reducedPersonActivities.add(reducedActivity);
		}

		return reducedPersonActivities;
	}

	private Map<Long, FriendsRecommendation> calcPersonSimilarities(List<PersonActivity> personActivities) {

		Map<Long, FriendsRecommendation> similarityMap = new HashMap<>();
		for (int i = 0; i < personActivities.size(); i++) {
			PersonActivity p1 = personActivities.get(i);
			List<PersonSimilarity> personSimilarities = new ArrayList<>();

			for (int j = 0; j < personActivities.size(); j++) {
				PersonActivity p2 = personActivities.get(j);
				String knowsQuery = Math.min(p1.personId(), p2.personId()) + "_"
						+ Math.max(p1.personId(), p2.personId());

				if (i != j && !knowsRelation.contains(knowsQuery)) {
					// ignore similarity between same person
					// filter out friends

					PersonSimilarity similarity = PersonSimilarity.dotProduct(p1, p2);

					personSimilarities.add(similarity);
				}
			}

			Comparator<PersonSimilarity> comp = new Comparator<PersonSimilarity>() {

				@Override
				public int compare(PersonSimilarity o1, PersonSimilarity o2) {
					int c = o1.similarity().compareTo(o2.similarity());
					if (c != 0) {
						return c;
					} else {
						return o1.person2Id().compareTo(o2.person2Id());
					}
				}

			};

			List<SimilarityTuple> top5Similarities = personSimilarities.stream()
					// .sorted(Comparator.comparingDouble(PersonSimilarity::similarity).reversed())
					.sorted(comp.reversed())
					.limit(5)
					.map(x -> new SimilarityTuple(x.person2Id(), x.similarity()))
					.collect(Collectors.toList());

			// create Friends recommendation
			FriendsRecommendation recommmendation = new FriendsRecommendation();
			recommmendation.setPersonId(p1.personId());
			recommmendation.setSimilarities(top5Similarities);

			similarityMap.put(recommmendation.getPersonId(), recommmendation);
		}

		return similarityMap;
	}

}
