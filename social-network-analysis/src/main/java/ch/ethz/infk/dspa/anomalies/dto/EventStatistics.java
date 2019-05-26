package ch.ethz.infk.dspa.anomalies.dto;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

public class EventStatistics {

	// store all votes of different features that correspond to an event
	private Set<Feature.FeatureId> votedFeatureIds;
	private Set<Feature> votesFraudulent;
	private Set<Feature> votesNonFraudulent;

	private Long personId;
	private String eventGUID;
	private Map<Feature.FeatureId, Double> thresholds;

	public EventStatistics(ImmutableMap<Feature.FeatureId, Double> thresholds) {
		votedFeatureIds = new HashSet<>();
		votesFraudulent = new HashSet<>();
		votesNonFraudulent = new HashSet<>();
		this.thresholds = new HashMap<>(thresholds);
	}

	// add a new vote based on a FeatureStatistics
	public EventStatistics addFeatureVote(FeatureStatistics featureStatistics) {
		// if the feature to be added has already "voted", throw
		if (this.votedFeatureIds.contains(featureStatistics.getFeatureId())) {
			throw new IllegalArgumentException("DUPLICATE_FEATURE_VOTE " + featureStatistics.getFeatureId());
		}

		if (this.personId == null) {
			this.personId = featureStatistics.getFeature().getPersonId();
		}

		if (this.eventGUID == null) {
			this.eventGUID = featureStatistics.getEventGUID();
		}

		// add the vote to the correct set depending on its decision
		if (featureStatistics.isAnomalous(this.thresholds)) {
			this.votesFraudulent.add(featureStatistics.getFeature());
		} else {
			this.votesNonFraudulent.add(featureStatistics.getFeature());
		}

		// add the voted feature id to prevent future duplicates
		this.votedFeatureIds.add(featureStatistics.getFeatureId());

		return this;
	}

	public Long getPersonId() {
		return this.personId;
	}

	public String getEventGUID() {
		return this.eventGUID;
	}

	public Set<Feature.FeatureId> getVotedFeatureIds() {
		return votedFeatureIds;
	}

	public Set<Feature> getVotesNonFraudulent() {
		return votesNonFraudulent;
	}

	public void setVotesNonFraudulent(Set<Feature> votesNonFraudulent) {
		this.votesNonFraudulent = votesNonFraudulent;
	}

	public Set<Feature> getVotesFraudulent() {
		return this.votesFraudulent;
	}

	// compute and return the ensemble decision based on a required percentage
	public boolean getIsAnomalousWithMajority(Double isFraudulentThreshold) {
		int numFraudulent = this.votesFraudulent.size();
		int numTotal = this.votesNonFraudulent.size() + numFraudulent;
		return (((double) numFraudulent) / numTotal) >= isFraudulentThreshold;
	}

	@Override
	public String toString() {
		return "EventStatistics{" +
				"personId=" + personId +
				", eventGUID='" + eventGUID + '\'' +
				", votedFeatureIds=" + votedFeatureIds +
				", votesFraudulent=" + votesFraudulent +
				", votesNonFraudulent=" + votesNonFraudulent +
				'}';
	}
}
