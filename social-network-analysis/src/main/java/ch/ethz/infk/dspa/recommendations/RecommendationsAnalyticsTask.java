package ch.ethz.infk.dspa.recommendations;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.ops.CategoryEnrichmentProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.FriendsFilterFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonActivityBroadcastJoinProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonActivityReduceFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonOutputSelectorProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.TopKAggregateFunction;
import ch.ethz.infk.dspa.stream.CommentDataStreamBuilder;
import ch.ethz.infk.dspa.stream.LikeDataStreamBuilder;
import ch.ethz.infk.dspa.stream.PostDataStreamBuilder;

import java.util.List;

public class RecommendationsAnalyticsTask extends AbstractAnalyticsTask<SingleOutputStreamOperator<List<PersonSimilarity>>, List<PersonSimilarity>> {

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
				.keyBy(activity -> 0L) // TODO [rsc] should be done differently if possible, don't want to send all to
										// same operator for output selection
				.process(new PersonOutputSelectorProcessFunction());

		BroadcastStream<PersonActivity> selectedPersonActivityStream = personActivityStream
				.getSideOutput(PersonOutputSelectorProcessFunction.SELECTED)
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
