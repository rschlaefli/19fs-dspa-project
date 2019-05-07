package ch.ethz.infk.dspa.anomalies.dto;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.math.Stats;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureStatistics {

    // define how many standard deviations around the mean are regarded as "normal"
    // TODO: extract this map somewhere else so we can parameterize
    public static final Map<Feature.FeatureId, Double> FEATURE_THRESHOLDS = ImmutableMap.of(
            Feature.FeatureId.TIMESTAMP, 1D,
            Feature.FeatureId.CONTENTS_LONG, 2D,
            Feature.FeatureId.CONTENTS_MEDIUM, 2D,
            Feature.FeatureId.CONTENTS_SHORT, 3D
    );

    private Feature feature;
    private Double rollingStd;
    private Double rollingMean;
    private long rollingCount;

    public FeatureStatistics(Feature currentFeature) {
        this.feature = currentFeature;
    }

    // compute the internal statistics based on a list of features
    public FeatureStatistics withStatisticsFrom(List<Feature> features) {
        // TODO: improve statistics computations (incremental)
        List<Double> values = features.stream().map(Feature::getFeatureValue).collect(Collectors.toList());
        Stats statistics = Stats.of(values);
        this.rollingMean = statistics.mean();
        this.rollingStd = statistics.populationStandardDeviation();
        this.rollingCount = statistics.count();
        return this;
    }

    public Double getRollingStd() {
        return this.rollingStd;
    }

    public Double getRollingMean() {
        return this.rollingMean;
    }

    public long getRollingCount() {
        return this.rollingCount;
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
        Double maximumDeviation = this.rollingStd * getApplicableThreshold();
        Double featureValue = this.feature.getFeatureValue();
        return featureValue < this.rollingMean - maximumDeviation || featureValue > this.rollingMean + maximumDeviation;
    }
}
