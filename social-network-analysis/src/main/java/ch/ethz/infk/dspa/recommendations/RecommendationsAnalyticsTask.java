package ch.ethz.infk.dspa.recommendations;

import java.util.List;

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
				.process(new CategoryEnrichmentProcessFunction())
				.keyBy(PersonActivity::personId)
				.window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
				.reduce(new PersonActivityReduceFunction())
				.process(new PersonOutputSelectorProcessFunction(10, Time.hours(1)));

		BroadcastStream<PersonActivity> selectedPersonActivityStream = personActivityStream
				.getSideOutput(PersonOutputSelectorProcessFunction.SELECTED)
				.process(new PersonOutputSelectorProcessFunction(10, Time.hours(1)))
				.setParallelism(1)
				.broadcast(PersonActivityBroadcastJoinProcessFunction.SELECTED_PERSON_STATE_DESCRIPTOR);

		this.outputStream = personActivityStream
				.connect(selectedPersonActivityStream)
				.process(new PersonActivityBroadcastJoinProcessFunction(10, Time.hours(1)))
				.filter(new FriendsFilterFunction(this.getStaticFilePath() + "person_knows_person.csv"))
				.keyBy(PersonSimilarity::person1Id)
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.aggregate(new TopKAggregateFunction(10));

		return this;
	}
}
