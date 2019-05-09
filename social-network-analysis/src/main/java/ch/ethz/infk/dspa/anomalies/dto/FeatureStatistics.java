package ch.ethz.infk.dspa.anomalies.dto;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class FeatureStatistics {

	// define how many standard deviations around the mean are regarded as "normal"
	// TODO: extract this map somewhere else so we can parameterize
	public static final Map<Feature.FeatureId, Double> FEATURE_THRESHOLDS = ImmutableMap.of(
			Feature.FeatureId.TIMESTAMP, 1D,
			Feature.FeatureId.CONTENTS_LONG, 2D,
			Feature.FeatureId.CONTENTS_MEDIUM, 2D,
			Feature.FeatureId.CONTENTS_SHORT, 3D);

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

	public Double getApplicableThreshold() {
		return FEATURE_THRESHOLDS.get(this.feature.getFeatureId());
	}

	public Feature getFeature() {
		return this.feature;
	}

	// compute and store whether the event is to be regarded as anomalous based on current statistics
	public boolean isAnomalous() {
		Double maximumDeviation = this.stdDev * getApplicableThreshold();
		Double featureValue = this.feature.getFeatureValue();
		return featureValue < this.mean - maximumDeviation || featureValue > this.mean + maximumDeviation;
	}
}
