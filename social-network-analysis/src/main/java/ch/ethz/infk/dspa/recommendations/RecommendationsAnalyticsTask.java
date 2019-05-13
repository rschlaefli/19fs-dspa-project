package ch.ethz.infk.dspa.recommendations;

import java.util.List;

import ch.ethz.infk.dspa.helper.Config;
import org.apache.commons.configuration2.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import ch.ethz.infk.dspa.recommendations.ops.CategoryEnrichmentProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.FriendsFilterFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonActivityBroadcastJoinProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonActivityReduceFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonOutputSelectorProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.TopKAggregateFunction;

public class RecommendationsAnalyticsTask
		extends AbstractAnalyticsTask<SingleOutputStreamOperator<List<PersonSimilarity>>, List<PersonSimilarity>> {

	@Override
	public RecommendationsAnalyticsTask initialize() throws Exception {
		this.withKafkaConsumerGroup("recommendations");
		super.initialize();
		return this;
	}

	@Override
	public RecommendationsAnalyticsTask build() {
		Time windowLength = Time.hours(this.config.getLong("tasks.recommendations.window.lengthInHours"));
		Time windowSlide = Time.hours(this.config.getLong("tasks.recommendations.window.slideInHours"));
		int selectionCount = this.config.getInt("tasks.recommendations.selectionCount");
		int topKCount = this.config.getInt("tasks.recommendations.topKCount");

		DataStream<PersonActivity> postPersonActivityStream = this.postStream
				.map(PersonActivity::of)
				.returns(PersonActivity.class);

		DataStream<PersonActivity> commentPersonActivityStream = this.commentStream
				.map(PersonActivity::of)
				.returns(PersonActivity.class);

		DataStream<PersonActivity> likePersonActivityStream = this.likeStream
				.map(PersonActivity::of)
				.returns(PersonActivity.class);

		SingleOutputStreamOperator<PersonActivity> personActivityStream = postPersonActivityStream
				.union(commentPersonActivityStream, likePersonActivityStream)
				.keyBy(PersonActivity::postId)
				.process(new CategoryEnrichmentProcessFunction(this.getStaticFilePath() + "forum_hasTag_tag.csv",
						this.getStaticFilePath() + "place_isPartOf_place.csv"))
				.keyBy(PersonActivity::personId)
				.window(SlidingEventTimeWindows.of(windowLength, windowSlide))
				.reduce(new PersonActivityReduceFunction())
				.process(new PersonOutputSelectorProcessFunction(selectionCount, windowSlide));

		BroadcastStream<PersonActivity> selectedPersonActivityStream = personActivityStream
				.getSideOutput(PersonOutputSelectorProcessFunction.SELECTED)
				.process(new PersonOutputSelectorProcessFunction(selectionCount, windowSlide))
				.setParallelism(1)
				.broadcast(PersonActivityBroadcastJoinProcessFunction.SELECTED_PERSON_STATE_DESCRIPTOR);

		this.outputStream = personActivityStream
				.connect(selectedPersonActivityStream)
				.process(new PersonActivityBroadcastJoinProcessFunction(selectionCount, windowSlide))
				.filter(new FriendsFilterFunction(
						this.getStaticFilePath() + "person_knows_person.csv"))
				.keyBy(PersonSimilarity::person1Id)
				.window(TumblingEventTimeWindows.of(windowSlide))
				.aggregate(new TopKAggregateFunction(topKCount));

		return this;
	}

	@Override
	public void start() throws Exception {
		super.start("Friends Recommendations");
	}
}
