package ch.ethz.infk.dspa.anomalies.ops;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import com.google.common.collect.Streams;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.google.common.collect.ImmutableMap;

import ch.ethz.infk.dspa.anomalies.dto.EventStatistics;
import ch.ethz.infk.dspa.anomalies.dto.Feature.EventType;
import ch.ethz.infk.dspa.anomalies.dto.Feature.FeatureId;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;

public class EnsembleProcessFunction extends KeyedProcessFunction<String, FeatureStatistics, EventStatistics> {

	private static final long serialVersionUID = 1L;

	// TODO Verify that all events are properly set here!
	private static final Set<FeatureId> contentFeatures = new HashSet<>(
			Arrays.asList(FeatureId.CONTENTS_SHORT, FeatureId.CONTENTS_MEDIUM, FeatureId.CONTENTS_LONG));

	private static final Set<FeatureId> mandatoryPostFeatures = new HashSet<>(
			Arrays.asList(FeatureId.TIMESPAN, FeatureId.TAG_COUNT));
	private static final Set<FeatureId> mandatoryCommentFeatures = new HashSet<>(
			Arrays.asList(FeatureId.TIMESPAN));
	private static final Set<FeatureId> mandatoryLikeFeatures = new HashSet<>(
			Arrays.asList(FeatureId.TIMESPAN, FeatureId.NEW_USER_LIKES, FeatureId.INTERACTIONS_RATIO));

	private final ImmutableMap<FeatureId, Double> thresholds;

	private ValueState<EventType> eventType;
	private MapState<FeatureId, FeatureStatistics> featureStatsMap;

	public EnsembleProcessFunction(ImmutableMap<FeatureId, Double> thresholds) {
		this.thresholds = thresholds;
	}

	@Override
	public void processElement(FeatureStatistics featureStats, Context ctx, Collector<EventStatistics> out)
			throws Exception {

		EventType eventType = featureStats.getFeature().getEventType();
		if (this.eventType.value() == null) {
			this.eventType.update(eventType);
		} else if (this.eventType.value() != eventType) {
			throw new IllegalArgumentException("Cannot change Event Type of feature");
		}

		// store feature statistic
		if (featureStatsMap.contains(featureStats.getFeatureId())) {
			throw new IllegalArgumentException("Duplicate Feature");
		}
		this.featureStatsMap.put(featureStats.getFeatureId(), featureStats);

		// check if all feature statistics of event type arrived
		Set<FeatureId> featureStatsKeySet = Streams.stream(this.featureStatsMap.keys()).collect(Collectors.toSet());
		if (hasAllFeatures(featureStatsKeySet, eventType)) {

			// build Event Statistics
			EventStatistics eventStats = new EventStatistics(this.thresholds);

			this.featureStatsMap.iterator().forEachRemaining(e -> {
				FeatureStatistics featureVote = e.getValue();
				eventStats.addFeatureVote(featureVote);
			});
			out.collect(eventStats);

			// clear state
			this.eventType.clear();
			this.featureStatsMap.clear();
		}

	}

	@Override
	public void open(Configuration parameters) throws Exception {
		MapStateDescriptor<FeatureId, FeatureStatistics> featureStatsMapDescriptor = new MapStateDescriptor<>(
				"anomalies-ensemble-featureStatsMap",
				TypeInformation.of(new TypeHint<FeatureId>() {
				}),
				TypeInformation.of(new TypeHint<FeatureStatistics>() {
				}));
		this.featureStatsMap = getRuntimeContext().getMapState(featureStatsMapDescriptor);

		ValueStateDescriptor<EventType> eventTypeDescriptor = new ValueStateDescriptor<>("anomalies-ensemble-eventType",
				TypeInformation.of(EventType.class));
		this.eventType = getRuntimeContext().getState(eventTypeDescriptor);
	}

	public static boolean hasAllFeatures(Set<FeatureId> featureStatsSet, EventType eventType) {
		switch (eventType) {
		case COMMENT:
			return containsOne(featureStatsSet, contentFeatures)
					&& containsAll(featureStatsSet, mandatoryCommentFeatures);
		case LIKE:
			return containsAll(featureStatsSet, mandatoryLikeFeatures);
		case POST:
			return containsOne(featureStatsSet, contentFeatures) && containsAll(featureStatsSet, mandatoryPostFeatures);
		default:
			throw new IllegalArgumentException("Unhandled EventType");
		}

	}

	private static boolean containsOne(Set<FeatureId> featureStatsSet,
			Set<FeatureId> expectedFeatures) {
		// checks if featureStatsMap contains at least one of the expectedFeatures
		return expectedFeatures.stream().anyMatch(featureStatsSet::contains);
	}

	private static boolean containsAll(Set<FeatureId> featureStatsSet,
			Set<FeatureId> expectedFeatures) {
		// checks if featureStatsMap contains all of the expectedFeatures
		return featureStatsSet.containsAll(expectedFeatures);
	}

}
