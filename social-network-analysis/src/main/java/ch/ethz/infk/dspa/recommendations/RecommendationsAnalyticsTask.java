package ch.ethz.infk.dspa.recommendations;

import ch.ethz.infk.dspa.recommendations.ops.*;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;

public class RecommendationsAnalyticsTask
		extends AbstractAnalyticsTask<SingleOutputStreamOperator<FriendsRecommendation>, FriendsRecommendation> {

	@Override
	public RecommendationsAnalyticsTask initialize() throws Exception {
		this.withKafkaConsumerGroup("recommendations");
		super.initialize();
		return this;
	}

	@Override
	public RecommendationsAnalyticsTask build() {
		final String staticForumHasTag = this.getStaticFilePath() + "forum_hasTag_tag.csv";
		final String staticPlaceIsPartOfPlace = this.getStaticFilePath() + "place_isPartOf_place.csv";
		final String staticTagHasTypeTagClass = this.getStaticFilePath() + "tag_hasType_tagclass.csv";
		final String staticTagClassIsSubClassOfTagClass = this.getStaticFilePath()
				+ "tagclass_isSubclassOf_tagclass.csv";
		final String staticPersonSpeaksLanguage = this.getStaticFilePath() + "person_speaks_language.csv";
		final String staticPersonHasInterest = this.getStaticFilePath() + "person_hasInterest_tag.csv";
		final String staticPersonIsLocatedIn = this.getStaticFilePath() + "person_isLocatedIn_place.csv";
		final String staticPersonWorkAt = this.getStaticFilePath() + "person_workAt_organisation.csv";
		final String staticPersonStudyAt = this.getStaticFilePath() + "person_studyAt_organisation.csv";
		final String staticPersonKnowsPerson = this.getStaticFilePath() + "person_knows_person.csv";
		final Time windowLength = Time.hours(this.config.getLong("tasks.recommendations.window.lengthInHours"));
		final Time windowSlide = Time.hours(this.config.getLong("tasks.recommendations.window.slideInHours"));
		final int selectionCount = this.config.getInt("tasks.recommendations.selectionCount");
		final int topKCount = this.config.getInt("tasks.recommendations.topKCount");

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
				.process(new CategoryEnrichmentProcessFunction(staticForumHasTag, staticPlaceIsPartOfPlace,
						staticTagHasTypeTagClass, staticTagClassIsSubClassOfTagClass))
				.keyBy(PersonActivity::personId)
				.window(SlidingEventTimeWindows.of(windowLength, windowSlide))
				.reduce(new PersonActivityReduceFunction())
				.map(new PersonEnrichmentRichMapFunction(staticPersonSpeaksLanguage, staticPersonHasInterest,
						staticPersonIsLocatedIn, staticPersonWorkAt, staticPersonStudyAt))
				.process(new PersonOutputSelectorProcessFunction(selectionCount, windowSlide));

		BroadcastStream<PersonActivity> selectedPersonActivityStream = personActivityStream
				.getSideOutput(PersonOutputSelectorProcessFunction.SELECTED)
				.process(new PersonOutputSelectorProcessFunction(selectionCount, windowSlide))
				.setParallelism(1)
				.broadcast(PersonActivityBroadcastJoinProcessFunction.SELECTED_PERSON_STATE_DESCRIPTOR);

		this.outputStream = personActivityStream
				.connect(selectedPersonActivityStream)
				.process(new PersonActivityBroadcastJoinProcessFunction(selectionCount, windowSlide))
				.filter(new FriendsFilterFunction(staticPersonKnowsPerson))
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
