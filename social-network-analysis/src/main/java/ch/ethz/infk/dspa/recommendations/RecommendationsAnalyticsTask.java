package ch.ethz.infk.dspa.recommendations;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import ch.ethz.infk.dspa.helper.StaticDataParser;
import ch.ethz.infk.dspa.recommendations.dto.FriendsRecommendation;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import ch.ethz.infk.dspa.recommendations.ops.CategoryEnrichmentProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.FriendsFilterFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonActivityAggregateFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonActivityBroadcastJoinProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.PersonNameMapFunction;
import ch.ethz.infk.dspa.recommendations.ops.StaticPersonActivityOutputProcessFunction;
import ch.ethz.infk.dspa.recommendations.ops.TopKAggregateFunction;
import ch.ethz.infk.dspa.recommendations.ops.WindowActivateProcessFunction;

public class RecommendationsAnalyticsTask
		extends AbstractAnalyticsTask<SingleOutputStreamOperator<FriendsRecommendation>, FriendsRecommendation> {

	private Set<Long> recommendationPersonIds;

	@Override
	public RecommendationsAnalyticsTask initialize() throws Exception {
		this.withKafkaConsumerGroup("recommendations");
		super.initialize();
		return this;
	}

	public RecommendationsAnalyticsTask withRecommendationPersonIds(Set<Long> recommendationPersonIds) {
		this.recommendationPersonIds = recommendationPersonIds;
		return this;
	}

	@Override
	public RecommendationsAnalyticsTask build() throws Exception {
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
		final String staticPerson = this.getStaticFilePath() + "person.csv";
		final Time windowLength = Time.hours(this.config.getLong("tasks.recommendations.window.lengthInHours"));
		final Time windowSlide = Time.hours(this.config.getLong("tasks.recommendations.window.slideInHours"));
		final int selectionCount = this.config.getInt("tasks.recommendations.selectionCount");
		final int topKCount = this.config.getInt("tasks.recommendations.topKCount");
		final boolean outputCategoryMap = this.config.getBoolean("tasks.recommendations.outputCategoryMap");

		if (this.recommendationPersonIds == null) {
			// if no personIds given, select selectionCount randomly from the static relation file
			List<Long> personIds = StaticDataParser.parseCsvFile(staticPerson)
					.map(tuple -> Long.parseLong(tuple.getField(0))).collect(Collectors.toList());

			Collections.shuffle(personIds);
			this.recommendationPersonIds = new HashSet<>(personIds.subList(0, selectionCount));
		}

		final Set<Long> recommendationPersonIds = this.recommendationPersonIds;

		DataStream<PersonActivity> postPersonActivityStream = this.postStream
				.map(PersonActivity::of)
				.returns(PersonActivity.class);

		DataStream<PersonActivity> commentPersonActivityStream = this.commentStream
				.map(PersonActivity::of)
				.returns(PersonActivity.class);

		DataStream<PersonActivity> likePersonActivityStream = this.likeStream
				.map(PersonActivity::of)
				.returns(PersonActivity.class);

		DataStream<PersonActivity> unionPersonActivityStream = postPersonActivityStream
				.union(commentPersonActivityStream, likePersonActivityStream);

		SingleOutputStreamOperator<PersonActivity> personActivityStream = unionPersonActivityStream
				.keyBy(PersonActivity::getPostId)
				.process(new CategoryEnrichmentProcessFunction(staticForumHasTag, staticPlaceIsPartOfPlace,
						staticTagHasTypeTagClass, staticTagClassIsSubClassOfTagClass))
				.keyBy(PersonActivity::getPersonId)
				.window(SlidingEventTimeWindows.of(windowLength, windowSlide))
				.aggregate(new PersonActivityAggregateFunction());

		DataStream<PersonActivity> staticPersonActivityStream = personActivityStream
				.process(new WindowActivateProcessFunction(windowSlide))
				.keyBy(x -> 0L)
				.process(new StaticPersonActivityOutputProcessFunction(windowSlide,
						staticPersonSpeaksLanguage,
						staticPersonHasInterest,
						staticPersonIsLocatedIn,
						staticPersonWorkAt,
						staticPersonStudyAt))
				.setParallelism(1);

		personActivityStream = personActivityStream
				.union(staticPersonActivityStream)
				.keyBy(PersonActivity::getPersonId)
				.window(TumblingEventTimeWindows.of(windowSlide))
				.aggregate(new PersonActivityAggregateFunction());

		BroadcastStream<PersonActivity> selectedPersonActivityStream = personActivityStream
				.filter(personActivity -> recommendationPersonIds.contains(personActivity.getPersonId()))
				.broadcast(PersonActivityBroadcastJoinProcessFunction.SELECTED_PERSON_STATE_DESCRIPTOR);

		this.outputStream = personActivityStream
				.connect(selectedPersonActivityStream)
				.process(new PersonActivityBroadcastJoinProcessFunction(selectionCount, windowSlide, outputCategoryMap))
				.filter(new FriendsFilterFunction(staticPersonKnowsPerson))
				.keyBy(PersonSimilarity::person1Id)
				.window(TumblingEventTimeWindows.of(windowSlide))
				.aggregate(new TopKAggregateFunction(topKCount))
				.map(new PersonNameMapFunction(staticPerson));

		return this;
	}

	@Override
	public void start() throws Exception {
		super.start("Friends Recommendations");
	}

	@Override
	protected Time getTumblingOutputWindow() {
		return Time.hours(this.config.getLong("tasks.recommendations.window.slideInHours"));
	}
}
