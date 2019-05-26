package ch.ethz.infk.dspa.anomalies.dto;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class FeatureStatistics {

	private Feature feature;
	private Double stdDev;
	private Double mean;

	public FeatureStatistics(Feature currentFeature) {
		this.feature = currentFeature;
	}

	public Double getStdDev() {
		return stdDev;
	}

	public void setStdDev(Double stdDev) {
		this.stdDev = stdDev;
	}

	public Double getMean() {
		return mean;
	}

	public void setMean(Double mean) {
		this.mean = mean;
	}

	public Feature.FeatureId getFeatureId() {
		return this.feature.getFeatureId();
	}

	public String getEventGUID() {
		return this.feature.getGUID();
	}

	public Feature getFeature() {
		return this.feature;
	}

	// compute and store whether the event is to be regarded as anomalous based on current statistics
	public boolean isAnomalous(Map<Feature.FeatureId, Double> thresholds) {
		Double currentFeatureThreshold = thresholds.get(feature.getFeatureId());
		Double maximumDeviation = this.stdDev * currentFeatureThreshold;
		Double featureValue = this.feature.getFeatureValue();
		return (featureValue < (this.mean - maximumDeviation)) || (featureValue > (this.mean + maximumDeviation));
	}

	@Override
	public String toString() {
		return "FeatureStatistics{" +
				"feature=" + feature +
				", stdDev=" + stdDev +
				", mean=" + mean +
				'}';
	}
}
