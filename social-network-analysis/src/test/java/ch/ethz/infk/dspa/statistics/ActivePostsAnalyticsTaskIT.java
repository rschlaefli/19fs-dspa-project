package ch.ethz.infk.dspa.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import ch.ethz.infk.dspa.statistics.dto.StatisticsOutput;
import ch.ethz.infk.dspa.statistics.dto.StatisticsOutput.OutputType;
import ch.ethz.infk.dspa.stream.helper.TestSink;

public class ActivePostsAnalyticsTaskIT extends AbstractAnalyticsTaskIT<StatisticsOutput> {

	@Override
	public void beforeEachTaskSpecific(List<Post> allPosts, List<Comment> allComments, List<Like> allLikes)
			throws Exception {
		// do nothing
	}

	@Override
	public List<StatisticsOutput> buildExpectedResultsOfWindow(WindowAssigner<Object, TimeWindow> assigner,
			TimeWindow timeWindow, List<Post> posts,
			List<Comment> comments,
			List<Like> likes) {

		SlidingEventTimeWindows slidingWindow = (SlidingEventTimeWindows) assigner;

		List<StatisticsOutput> statisticOutputs;

		if (slidingWindow.getSlide() == Time.minutes(30).toMilliseconds()) {
			statisticOutputs = buildExpectedCommentReplyCountResultsOfWindow(posts, comments, likes);
		} else if (slidingWindow.getSlide() == Time.hours(1).toMilliseconds()) {
			statisticOutputs = buildExpectedUniquePeopleCountResultsOfWindow(posts, comments, likes);
		} else {
			throw new IllegalArgumentException("UNEXPECTED_TEST_SCENARIO");
		}

		return statisticOutputs;

	}

	@Override
	public List<ResultWindow<StatisticsOutput>> produceActualResults(DataStream<Post> posts,
			DataStream<Comment> comments, DataStream<Like> likes) throws Exception {

		ActivePostsAnalyticsTask analyticsTask = (ActivePostsAnalyticsTask) new ActivePostsAnalyticsTask()
				.withPropertiesConfiguration(getConfig())
				.withStreamingEnvironment(getEnv())
				.withMaxDelay(getMaxOutOfOrderness())
				.withInputStreams(posts, comments, likes)
				.initialize()
				.build()
				.withSink(new TestSink<>());

		analyticsTask.start();

		List<ResultWindow<StatisticsOutput>> resultWindows1 = TestSink.getResultsInResultWindow(StatisticsOutput.class,
				SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)),
				t -> t.getOutputType() == OutputType.COMMENT_COUNT
						|| t.getOutputType() == OutputType.REPLY_COUNT);

		List<ResultWindow<StatisticsOutput>> resultWindows2 = TestSink.getResultsInResultWindow(StatisticsOutput.class,
				SlidingEventTimeWindows.of(Time.hours(12), Time.hours(1)),
				t -> t.getOutputType() == OutputType.UNIQUE_PERSON_COUNT);

		List<ResultWindow<StatisticsOutput>> resultWindows = new ArrayList<>();
		resultWindows.addAll(resultWindows1);
		resultWindows.addAll(resultWindows2);

		Map<TimeWindow, List<StatisticsOutput>> map = new HashMap<>();

		for (ResultWindow<StatisticsOutput> resultWindow : resultWindows) {
			TimeWindow window = new TimeWindow(resultWindow.getStart(), resultWindow.getEnd());
			List<StatisticsOutput> results = map.getOrDefault(window, new ArrayList<>());
			results.addAll(resultWindow.getResults());
			map.put(window, results);
		}

		resultWindows = map.entrySet().stream()
				.filter(e -> e.getValue().size() != 0)
				.map(e -> {
					ResultWindow<StatisticsOutput> w = ResultWindow.of(e.getKey());
					w.setResults(e.getValue());
					return w;
				}).collect(Collectors.toList());

		return resultWindows;
	}

	private List<StatisticsOutput> buildExpectedCommentReplyCountResultsOfWindow(List<Post> posts,
			List<Comment> comments, List<Like> likes) {

		Set<Long> activePosts = getActivePostsOfWindow(posts, comments, likes);

		List<Comment> replies = comments.stream()
				.filter(comment -> comment.getReplyToCommentId() != null)
				.collect(Collectors.toList());
		List<Comment> rootComments = comments.stream()
				.filter(comment -> comment.getReplyToCommentId() == null)
				.collect(Collectors.toList());

		List<StatisticsOutput> results = new ArrayList<>();

		for (Long postId : activePosts) {
			long numberOfReplies = replies.stream()
					.filter(comment -> postId.equals(comment.getReplyToPostId()))
					.count();

			StatisticsOutput replyCountOutput = new StatisticsOutput(postId, numberOfReplies, OutputType.REPLY_COUNT);
			results.add(replyCountOutput);

			long numberOfRootComments = rootComments.stream()
					.filter(comment -> postId.equals(comment.getReplyToPostId()))
					.count();

			StatisticsOutput commentCountOutput = new StatisticsOutput(postId, numberOfRootComments,
					OutputType.COMMENT_COUNT);
			results.add(commentCountOutput);

		}
		return results;
	}

	private List<StatisticsOutput> buildExpectedUniquePeopleCountResultsOfWindow(List<Post> posts,
			List<Comment> comments, List<Like> likes) {

		Set<Long> activePosts = getActivePostsOfWindow(posts, comments, likes);

		List<StatisticsOutput> results = new ArrayList<>();
		for (Long postId : activePosts) {

			Set<Long> personIds1 = posts.stream()
					.filter(post -> postId.equals(post.getId()))
					.map(Post::getPersonId)
					.collect(Collectors.toSet());

			Set<Long> personIds2 = comments.stream()
					.filter(comment -> postId.equals(comment.getReplyToPostId()))
					.map(Comment::getPersonId)
					.collect(Collectors.toSet());

			Set<Long> personIds3 = likes.stream()
					.filter(like -> postId.equals(like.getPostId()))
					.map(Like::getPersonId)
					.collect(Collectors.toSet());

			Set<Long> personIds = new HashSet<>();
			personIds.addAll(personIds1);
			personIds.addAll(personIds2);
			personIds.addAll(personIds3);

			long uniquePeopleCount = personIds.size();

			if (uniquePeopleCount > 0) {

				StatisticsOutput peopleCountOutput = new StatisticsOutput(postId, uniquePeopleCount,
						OutputType.UNIQUE_PERSON_COUNT);

				results.add(peopleCountOutput);
			}

		}

		return results;
	}

	private Set<Long> getActivePostsOfWindow(List<Post> posts, List<Comment> comments, List<Like> likes) {

		Set<Long> postIds = new HashSet<>();

		Set<Long> postIds1 = posts.stream()
				.map(Post::getId)
				.collect(Collectors.toSet());
		Set<Long> postIds2 = comments.stream()
				.map(Comment::getReplyToPostId)
				.collect(Collectors.toSet());
		Set<Long> postIds3 = likes.stream()
				.map(Like::getPostId)
				.collect(Collectors.toSet());

		postIds.addAll(postIds1);
		postIds.addAll(postIds2);
		postIds.addAll(postIds3);

		return postIds;

	}

	@Override
	public List<WindowAssigner<Object, TimeWindow>> getWindowAssigners() {
		List<WindowAssigner<Object, TimeWindow>> assigners = new ArrayList<>();

		assigners.add(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)));
		assigners.add(SlidingEventTimeWindows.of(Time.hours(12), Time.hours(1)));

		return assigners;
	}

	@Override
	public Time getMaxOutOfOrderness() {
		return Time.seconds(600);
	}

}
