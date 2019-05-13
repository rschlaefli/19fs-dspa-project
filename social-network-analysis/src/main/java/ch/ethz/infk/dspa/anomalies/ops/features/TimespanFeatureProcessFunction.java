package ch.ethz.infk.dspa.anomalies.ops.features;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infk.dspa.anomalies.dto.Feature;

public class TimespanFeatureProcessFunction extends KeyedProcessFunction<Long, Feature, Feature> {

	private static final long serialVersionUID = 1L;

	// store the maximum timestamp that was seen before
	// if a feature arrives with event time larger than this but smaller than the watermark
	// we can return the feature with a feature value set to the difference
	// otherwise, we need to buffer the event until the watermark is at least equal
	private MapState<Long, List<Feature>> featureMapState;
	private ValueState<Long> lastEventBeforeWatermarkState;

	public DataStream<Feature> applyTo(DataStream<Feature> postInputStream,
			DataStream<Feature> commentFeatureStream, DataStream<Feature> likeInputStream) {
		return postInputStream
				.union(commentFeatureStream, likeInputStream)
				.keyBy(Feature::getPersonId)
				.process(this);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.featureMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>(
				"TimestampFeatureProcessState",
				BasicTypeInfo.LONG_TYPE_INFO,
				TypeInformation.of(new TypeHint<List<Feature>>() {
				})));
		this.lastEventBeforeWatermarkState = getRuntimeContext()
				.getState(new ValueStateDescriptor<>("LastEventBeforeWatermark", BasicTypeInfo.LONG_TYPE_INFO));
	}

	@Override
	public void processElement(Feature feature, Context ctx, Collector<Feature> out) throws Exception {
		// set the feature id of the feature
		feature.withFeatureId(Feature.FeatureId.TIMESPAN);

		// extract a list of all features for the current timestamp
		List<Feature> currentFeatures = this.featureMapState.get(ctx.timestamp());

		// if the watermark has not yet progressed to the state of the current feature
		// add it to the buffer and set a new timer
		if (currentFeatures == null) {
			currentFeatures = new ArrayList<>();
		}

		currentFeatures.add(feature);
		this.featureMapState.put(ctx.timestamp(), currentFeatures);

		ctx.timerService().registerEventTimeTimer(ctx.timestamp());
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Feature> out) throws Exception {
		List<Feature> currentFeatures = this.featureMapState.get(timestamp);

		// get the next smaller maximum timestamp that is in the buffer
		Long maxBufferedTs = this.lastEventBeforeWatermarkState.value();
		Double tsDifference = maxBufferedTs != null ? timestamp - maxBufferedTs.doubleValue() : 0;

		// update the maximum timestamp that has been seen
		this.lastEventBeforeWatermarkState.update(timestamp);

		// for all features stored at the current timestamp
		// map them to include the difference to the above mamimum timestamp
		currentFeatures.forEach(f -> {
			f.withFeatureValue(tsDifference);
			out.collect(f);
		});

		// remove all entries for the current timestamp
		// they will not be needed anymore
		this.featureMapState.remove(timestamp);
	}
}
