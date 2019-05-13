package ch.ethz.infk.dspa.anomalies.ops;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.FeatureStatistics;
import ch.ethz.infk.dspa.helper.function.DoubleSumReduceFunction;
import ch.ethz.infk.dspa.helper.function.LongSumReduceFunction;

/**
 * ProcessFunction implementing Welford's online algorithm to keep an online mean and
 * variance/stdDev
 */
public class OnlineAverageProcessFunction
		extends KeyedProcessFunction<Feature.FeatureId, Feature, FeatureStatistics> {

	private static final long serialVersionUID = 1L;

	ReducingState<Long> count;
	ReducingState<Double> mean;
	ReducingState<Double> m2;

	MapState<Long, Set<Feature>> buffer;

	@Override
	public void processElement(Feature feature, Context ctx, Collector<FeatureStatistics> out) throws Exception {

		long timestamp = ctx.timestamp();

		Set<Feature> bufferedFeatures = ObjectUtils.defaultIfNull(buffer.get(timestamp), new HashSet<Feature>());
		bufferedFeatures.add(feature);
		buffer.put(timestamp, bufferedFeatures);

		ctx.timerService().registerEventTimeTimer(timestamp);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<FeatureStatistics> out) throws Exception {

		// clear buffer
		Set<Feature> bufferedFeatures = buffer.get(timestamp);
		buffer.remove(timestamp);

		// update the rolling average with all features from this timestamp
		for (Feature feature : bufferedFeatures) {
			updateRollingStats(feature.getFeatureValue());
		}

		// output each feature along with the updated rolling statistics
		for (Feature feature : bufferedFeatures) {

			FeatureStatistics featureStat = new FeatureStatistics(feature);

			featureStat.setMean(getMean());
			featureStat.setStdDev(getStdDev());
			out.collect(featureStat);
		}

	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		// initialize value states

		ReducingStateDescriptor<Long> countDescriptor = new ReducingStateDescriptor<>("anomalies-onlineavg-count",
				new LongSumReduceFunction(), Long.class);
		this.count = getRuntimeContext().getReducingState(countDescriptor);

		ReducingStateDescriptor<Double> m2Descriptor = new ReducingStateDescriptor<>("anomalies-onlineavg-m2",
				new DoubleSumReduceFunction(), Double.class);

		this.m2 = getRuntimeContext().getReducingState(m2Descriptor);

		ReducingStateDescriptor<Double> meanDescriptor = new ReducingStateDescriptor<>("anomalies-onlineavg-mean",
				new DoubleSumReduceFunction(), Double.class);

		this.mean = getRuntimeContext().getReducingState(meanDescriptor);

		MapStateDescriptor<Long, Set<Feature>> bufferDescriptor = new MapStateDescriptor<>(
				"anomalies-onlineavg-buffer",
				BasicTypeInfo.LONG_TYPE_INFO,
				TypeInformation.of(new TypeHint<Set<Feature>>() {
				}));

		this.buffer = getRuntimeContext().getMapState(bufferDescriptor);

	}

	private void updateRollingStats(double newMeasurement) throws Exception {

		count.add(1L);

		double delta1 = newMeasurement - ObjectUtils.defaultIfNull(mean.get(), 0.0);
		mean.add(delta1 / count.get());

		double delta2 = newMeasurement - mean.get();
		m2.add(delta1 * delta2);
	}

	private double getMean() throws Exception {
		return mean.get();
	}

	private double getStdDev() throws Exception {
		double variance = m2.get() / count.get();
		return Math.sqrt(variance);
	}

}
