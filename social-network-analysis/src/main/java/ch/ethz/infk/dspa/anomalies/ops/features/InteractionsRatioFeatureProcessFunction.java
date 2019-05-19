package ch.ethz.infk.dspa.anomalies.ops.features;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.helper.function.LongSumReduceFunction;
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
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class InteractionsRatioFeatureProcessFunction extends KeyedProcessFunction<Long, Feature, Feature> {

	private static final long serialVersionUID = 1L;

	private ReducingState<Long> commentCount;
	private ReducingState<Long> likeCount;
	private MapState<Long, List<Feature>> buffer;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.commentCount = getRuntimeContext()
				.getReducingState(new ReducingStateDescriptor<>("anomalies-interactionsratio-commentcount",
						new LongSumReduceFunction(), BasicTypeInfo.LONG_TYPE_INFO));

		this.likeCount = getRuntimeContext()
				.getReducingState(new ReducingStateDescriptor<>("anomalies-interactionsratio-likecount",
						new LongSumReduceFunction(), BasicTypeInfo.LONG_TYPE_INFO));

		this.buffer = getRuntimeContext().getMapState(new MapStateDescriptor<>("anomalies-interactionsratio-buffer",
				BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<List<Feature>>() {
				})));
	}

	@Override
	public void processElement(Feature in, Context ctx, Collector<Feature> out) throws Exception {
		// buffer incoming events because these events need to be processed in event time order
		List<Feature> features = ObjectUtils.defaultIfNull(buffer.get(ctx.timestamp()), new ArrayList<>());
		features.add(in);
		buffer.put(ctx.timestamp(), features);
		ctx.timerService().registerEventTimeTimer(ctx.timestamp());
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Feature> out) throws Exception {
		List<Feature> features = buffer.get(timestamp);
		buffer.remove(timestamp);

		// process all features for the comment and like counts at the current timestamp
		for (Feature feature : features) {
			if (feature.getEventType().equals(Feature.EventType.COMMENT)) {
				this.commentCount.add(1L);
			} else if (feature.getEventType().equals(Feature.EventType.LIKE)) {
				this.likeCount.add(1L);
			} else {
				throw new IllegalArgumentException("CANNOT_PROCESS_EVENT_FOR_INTERACTIONS");
			}
		}

		// compute the interactions ratio for the current state
		double likeToCommentRatio;
		if (this.likeCount.get() != null && this.commentCount.get() != null) {
			likeToCommentRatio = this.likeCount.get().doubleValue() / this.commentCount.get();
		} else if (this.commentCount.get() == null) {
			likeToCommentRatio = 1.0;
		} else {
			likeToCommentRatio = 0.0;
		}

		// output all current features with the interactions ratio up to the current point in time
		features.stream()
				.filter(feature -> feature.getEventType().equals(Feature.EventType.LIKE))
				.forEach(feature -> out.collect(feature
						.withFeatureId(Feature.FeatureId.INTERACTIONS_RATIO)
						.withFeatureValue(likeToCommentRatio)));
	}

	public DataStream<Feature> applyTo(DataStream<Feature> commentInputStream, DataStream<Feature> likeInputStream) {
		return commentInputStream
				.union(likeInputStream)
				.keyBy(Feature::getPostId)
				.process(this);
	}
}
