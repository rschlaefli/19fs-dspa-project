package ch.ethz.infk.dspa.anomalies.ops.features;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.Feature.FeatureId;
import ch.ethz.infk.dspa.helper.StaticDataParser;
import ch.ethz.infk.dspa.helper.function.LongSumReduceFunction;
import ch.ethz.infk.dspa.helper.tuple.NTuple2;
import ch.ethz.infk.dspa.helper.tuple.NTuple8;

public class NewUserInteractionFeatureProcessFunction extends KeyedProcessFunction<Long, Feature, Feature> {

	private static final long serialVersionUID = 1L;

	private ReducingState<Long> newUserLikeCount;
	private ReducingState<Long> totalLikeCount;
	private MapState<Long, List<Feature>> buffer;

	private final String personRelationFile;
	private Map<Long, Long> userCreationMap = new HashMap<>();
	private final long newUserThreshold;

	public NewUserInteractionFeatureProcessFunction(Time newUserThreshold, String personRelationFile) {
		this.newUserThreshold = newUserThreshold.toMilliseconds();
		this.personRelationFile = personRelationFile;
	}

	@Override
	public void processElement(Feature feature, Context ctx, Collector<Feature> out) throws Exception {
		// buffer incoming events because these events need to be processed in event time order
		List<Feature> features = ObjectUtils.defaultIfNull(buffer.get(ctx.timestamp()), new ArrayList<Feature>());
		features.add(feature);
		buffer.put(ctx.timestamp(), features);
		ctx.timerService().registerEventTimeTimer(ctx.timestamp());
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Feature> out) throws Exception {

		List<Feature> features = buffer.get(timestamp);
		buffer.remove(timestamp);

		// update counts depending on newUserThreshold
		for (Feature feature : features) {

			Long personId = feature.getPersonId();

			Long likeCreationTimestamp = feature.getLike().getCreationDate().getMillis();
			Long userCreationTimestamp = userCreationMap.get(personId);

			if (userCreationTimestamp == null) {
				System.out.println("WARNING: Person " + personId + " not found");
				userCreationTimestamp = 0L;
			}

			Long creationInteractionDifference = likeCreationTimestamp - userCreationTimestamp;
			if (creationInteractionDifference < newUserThreshold) {
				newUserLikeCount.add(1L);
			}

			totalLikeCount.add(1L);
		}

		// output all features with feature value
		for (Feature feature : features) {
			double newUserLikeCountDouble = ObjectUtils.defaultIfNull(newUserLikeCount.get(), 0.0).doubleValue();
			double newUserLikePercentage = newUserLikeCountDouble / totalLikeCount.get();
			feature = feature.withFeatureId(FeatureId.NEW_USER_LIKES).withFeatureValue(newUserLikePercentage);

			out.collect(feature);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		userCreationMap = buildUserCreationRelation(this.personRelationFile);

		ReducingStateDescriptor<Long> newUserLikeCountDescriptor = new ReducingStateDescriptor<>(
				"anomalies-newuserlikecount", new LongSumReduceFunction(), Long.class);

		this.newUserLikeCount = getRuntimeContext().getReducingState(newUserLikeCountDescriptor);

		ReducingStateDescriptor<Long> totalLikeCountDescriptor = new ReducingStateDescriptor<>(
				"anomalies-totallikecount", new LongSumReduceFunction(), Long.class);
		this.totalLikeCount = getRuntimeContext().getReducingState(totalLikeCountDescriptor);

		MapStateDescriptor<Long, List<Feature>> bufferDescriptor = new MapStateDescriptor<>(
				"anomalies-newuserinteraction-buffer",
				BasicTypeInfo.LONG_TYPE_INFO,
				TypeInformation.of(new TypeHint<List<Feature>>() {
				}));

		this.buffer = getRuntimeContext().getMapState(bufferDescriptor);

	}

	public DataStream<Feature> applyTo(DataStream<Feature> likeInputStream) {
		return likeInputStream.keyBy(feature -> feature.getLike().getPostId())
				.process(this);
	}

	public static Map<Long, Long> buildUserCreationRelation(String personRelationFile) throws IOException {
		Map<Long, Long> userCreationRelation = StaticDataParser
				.parseCsvFile(personRelationFile,
						Arrays.asList("id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP",
								"browserUsed"))
				.map(tuple -> (NTuple8<String, String, String, String, String, String, String, String>) tuple)
				.map(tuple -> NTuple2.of("personId", Long.valueOf(tuple.get("id")), "creationTimestamp", new DateTime(
						ZonedDateTime.parse(tuple.get("creationDate")).toInstant().toEpochMilli()).getMillis()))
				.collect(Collectors.toMap(t -> t.get("personId"), t -> t.get("creationTimestamp")));

		return userCreationRelation;
	}

}
