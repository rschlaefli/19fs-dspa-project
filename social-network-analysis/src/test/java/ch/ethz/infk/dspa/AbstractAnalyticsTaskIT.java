package ch.ethz.infk.dspa;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.helper.Config;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.AbstractTestDataGenerator.TestDataPair;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.LikeTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;

public abstract class AbstractAnalyticsTaskIT<OUT_TYPE> extends AbstractTestBase {

	private Configuration config;
	private StreamExecutionEnvironment env;
	private String staticFilePath = "src/test/java/resources/relations/";
	private DataStream<Post> postStream;
	private DataStream<Comment> commentStream;
	private DataStream<Like> likeStream;

	private List<Post> posts;
	private List<Comment> comments;
	private List<Like> likes;

	@BeforeEach
	public void beforeEach() throws Exception {

		// reset the test sink
		TestSink.reset();

		// setup config and environment
		config = Config.getConfig("src/main/java/ch/ethz/infk/dspa/config.properties");
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// setup all streams
		PostTestDataGenerator postGenerator = new PostTestDataGenerator();
		postStream = postGenerator
				.generate(env, "src/test/java/resources/post_event_stream.csv", getMaxOutOfOrderness());
		posts = postGenerator.getTestData().stream().map(TestDataPair::getElement)
				.sorted(Comparator.comparing(Post::getCreationDate)).collect(Collectors.toList());

		CommentTestDataGenerator commentGenerator = new CommentTestDataGenerator();
		commentStream = commentGenerator
				.generate(env, "src/test/java/resources/comment_event_stream.csv", getMaxOutOfOrderness());
		comments = commentGenerator.getTestData().stream().map(TestDataPair::getElement)
				.sorted(Comparator.comparing(Comment::getCreationDate)).collect(Collectors.toList());
		comments = addCommentPostMapping(comments);

		LikeTestDataGenerator likeGenerator = new LikeTestDataGenerator();
		likeStream = likeGenerator
				.generate(env, "src/test/java/resources/likes_event_stream.csv", getMaxOutOfOrderness());
		likes = likeGenerator.getTestData().stream().map(TestDataPair::getElement)
				.sorted(Comparator.comparing(Like::getCreationDate)).collect(Collectors.toList());

		beforeEachTaskSpecific(posts, comments, likes);

	}

	@Test
	public void test() throws Exception {

		// 2nd Assign Test Data to Windows
		List<WindowAssigner<Object, TimeWindow>> assigners = getWindowAssigners();

		// 3.1 Calculate Expected Results
		List<ResultWindow<OUT_TYPE>> expectedWindows = new ArrayList<>();
		for (WindowAssigner<Object, TimeWindow> assigner : assigners) {

			expectedWindows.addAll(assignToWindows(posts, comments, likes, assigner));

			for (ResultWindow<OUT_TYPE> expectedWindow : expectedWindows) {
				List<OUT_TYPE> expectedResults = buildExpectedResultsOfWindow(assigner, expectedWindow,
						expectedWindow.getPosts(),
						expectedWindow.getComments(),
						expectedWindow.getLikes());
				expectedWindow.setResults(expectedResults);
			}

		}

		// 3.2 Merge Expected Results
		Map<TimeWindow, List<ResultWindow<OUT_TYPE>>> map = expectedWindows.stream()
				.collect(Collectors.groupingBy(x -> new TimeWindow(x.getStart(), x.getEnd()), Collectors.toList()));
		// clear old list
		expectedWindows = new ArrayList<>();
		for (Entry<TimeWindow, List<ResultWindow<OUT_TYPE>>> entry : map.entrySet()) {

			ResultWindow<OUT_TYPE> mergedResultWindow = ResultWindow.of(entry.getKey());

			List<ResultWindow<OUT_TYPE>> resultWindows = entry.getValue();

			mergedResultWindow.setPosts(resultWindows.get(0).getPosts());
			mergedResultWindow.setComments(resultWindows.get(0).getComments());
			mergedResultWindow.setLikes(resultWindows.get(0).getLikes());

			List<OUT_TYPE> mergedResults = resultWindows.stream()
					.map(ResultWindow::getResults)
					.flatMap(List::stream)
					.collect(Collectors.toList());
			mergedResultWindow.setResults(mergedResults);

			expectedWindows.add(mergedResultWindow);
		}

		// sort by start of window
		expectedWindows = expectedWindows.stream()
				.sorted(Comparator.comparing(ResultWindow::getStart))
				.collect(Collectors.toList());

		// 4th Calculate Actual Results
		List<ResultWindow<OUT_TYPE>> actualWindows = produceActualResults(postStream, commentStream, likeStream);

		// 5th Check Results
		checkTimeWindows(expectedWindows, actualWindows);

		checkWindowContents(expectedWindows, actualWindows);

	}

	public abstract void beforeEachTaskSpecific(List<Post> allPosts, List<Comment> allComments, List<Like> allLikes)
			throws Exception;

	public abstract List<OUT_TYPE> buildExpectedResultsOfWindow(WindowAssigner<Object, TimeWindow> assigner,
			TimeWindow window, List<Post> posts,
			List<Comment> comments,
			List<Like> likes) throws Exception;

	public abstract List<ResultWindow<OUT_TYPE>> produceActualResults(DataStream<Post> posts,
			DataStream<Comment> comments, DataStream<Like> likes) throws Exception;

	public abstract List<WindowAssigner<Object, TimeWindow>> getWindowAssigners();

	public abstract Time getMaxOutOfOrderness();

	public Configuration getConfig() {
		return config;
	}

	public StreamExecutionEnvironment getEnv() {
		return env;
	}

	public String getStaticFilePath() {
		return staticFilePath;
	}

	/**
	 * Expects that the windows are identical and in the same order. Additionally requires a proper equals
	 * implementation of the OUT_TYPE. Then it checks if also the contents matches.
	 * 
	 * @param expectedWindows
	 * @param actualWindows
	 */
	private void checkWindowContents(List<ResultWindow<OUT_TYPE>> expectedWindows,
			List<ResultWindow<OUT_TYPE>> actualWindows) {
		for (int i = 0; i < expectedWindows.size(); i++) {
			ResultWindow<OUT_TYPE> expectedWindow = expectedWindows.get(i);
			ResultWindow<OUT_TYPE> actualWindow = actualWindows.get(i);

			List<OUT_TYPE> expectedResults = expectedWindow.getResults();
			List<OUT_TYPE> expectedResultsImmutable = ImmutableList.copyOf(expectedResults);

			List<OUT_TYPE> actualResults = actualWindow.getResults();
			List<OUT_TYPE> actualResultsImmutable = ImmutableList.copyOf(actualResults);

			// calculate symmetric difference
			for (OUT_TYPE x : actualResultsImmutable) {
				// removes first occurence of x in expectedResults
				expectedResults.remove(x);
			}
			for (OUT_TYPE x : expectedResultsImmutable) {
				// removes first occurence of x in actualResults
				actualResults.remove(x);
			}

			assertEquals(Collections.emptyList(), expectedResults,
					"Expected more Results than Actual: Window=" + expectedWindow.getStart());
			assertEquals(Collections.emptyList(), actualResults,
					"More Actual Results than Expected: Window=" + expectedWindow.getStart());
		}
	}

	/**
	 * Checks that the actual windows match the expected windows but does not check the results within the windows
	 * 
	 * @param expectedWindows
	 * @param actualWindows
	 */
	private void checkTimeWindows(List<ResultWindow<OUT_TYPE>> expectedWindows,
			List<ResultWindow<OUT_TYPE>> actualWindows) {

		List<TimeWindow> expectedTimeWindows = expectedWindows.stream()
				.map(window -> new TimeWindow(window.getStart(), window.getEnd()))
				.collect(Collectors.toList());

		List<TimeWindow> expectedTimeWindowsImmutable = ImmutableList.copyOf(expectedTimeWindows);

		List<TimeWindow> actualTimeWindows = actualWindows.stream()
				.map(window -> new TimeWindow(window.getStart(), window.getEnd()))
				.collect(Collectors.toList());

		List<TimeWindow> actualTimeWindowsImmutable = ImmutableList.copyOf(actualTimeWindows);

		// calculate symmetric difference
		for (TimeWindow x : actualTimeWindowsImmutable) {
			// removes first occurence of x in expectedTimeWindows
			expectedTimeWindows.remove(x);
		}
		for (TimeWindow x : expectedTimeWindowsImmutable) {
			// removes first occurence of x in actualTimeWindows
			actualTimeWindows.remove(x);
		}

		assertEquals(Collections.emptyList(), expectedTimeWindows, "Expected more Windows than in the Result");
		assertEquals(Collections.emptyList(), actualTimeWindows, "More Windows in the Result than Expected");

	}

	public List<ResultWindow<OUT_TYPE>> assignToWindows(List<Post> posts, List<Comment> comments,
			List<Like> likes, WindowAssigner<Object, TimeWindow> assigner) {

		// assign posts, comments and likes to windows
		Map<TimeWindow, List<Post>> windowedPosts = new HashMap<>();
		Map<TimeWindow, List<Comment>> windowedComments = new HashMap<>();
		Map<TimeWindow, List<Like>> windowedLikes = new HashMap<>();

		for (Post post : posts) {
			Collection<TimeWindow> windows = assigner.assignWindows(post, post.getCreationDate().getMillis(), null);
			for (TimeWindow window : windows) {
				if (!windowedPosts.containsKey(window)) {
					windowedPosts.put(window, new ArrayList<>());
				}
				windowedPosts.get(window).add(post);
			}
		}

		for (Comment comment : comments) {
			Collection<TimeWindow> windows = assigner.assignWindows(comment, comment.getCreationDate().getMillis(),
					null);
			for (TimeWindow window : windows) {
				if (!windowedComments.containsKey(window)) {
					windowedComments.put(window, new ArrayList<>());
				}
				windowedComments.get(window).add(comment);
			}
		}

		for (Like like : likes) {
			Collection<TimeWindow> windows = assigner.assignWindows(like, like.getCreationDate().getMillis(), null);
			for (TimeWindow window : windows) {
				if (!windowedLikes.containsKey(window)) {
					windowedLikes.put(window, new ArrayList<>());
				}
				windowedLikes.get(window).add(like);
			}
		}

		// find all existing time windows
		Set<TimeWindow> timeWindows = new HashSet<>();
		timeWindows.addAll(windowedPosts.keySet());
		timeWindows.addAll(windowedComments.keySet());
		timeWindows.addAll(windowedLikes.keySet());

		// convert to result windows
		List<ResultWindow<OUT_TYPE>> resultWindows = new ArrayList<>();

		for (TimeWindow timeWindow : timeWindows) {
			ResultWindow<OUT_TYPE> resultWindow = ResultWindow.of(timeWindow);

			resultWindow.setPosts(windowedPosts.getOrDefault(timeWindow, new ArrayList<>()));
			resultWindow.setComments(windowedComments.getOrDefault(timeWindow, new ArrayList<>()));
			resultWindow.setLikes(windowedLikes.getOrDefault(timeWindow, new ArrayList<>()));

			resultWindows.add(resultWindow);
		}

		// sort by start of window
		resultWindows = resultWindows.stream()
				.sorted(Comparator.comparing(ResultWindow::getStart))
				.collect(Collectors.toList());

		return resultWindows;

	}

	private List<Comment> addCommentPostMapping(List<Comment> comments) {
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
		return comments;
	}

}
