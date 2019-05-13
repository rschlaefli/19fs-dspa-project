package ch.ethz.infk.dspa.anomalies.dto;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.java.tuple.Tuple2;

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
			// TODO: we can only enable this once the rolling average has been implemented
			// as currently it generates multiple
			System.out.println(this.votedFeatureIds + "_" + featureStatistics.getFeatureId() + "_" + eventGUID);
			// throw new IllegalArgumentException("DUPLICATE_FEATURE_VOTE " + featureStatistics.getFeatureId());
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

	public Tuple2<Set<Feature>, Set<Feature>> getFeatureVotes() {
		return Tuple2.of(this.votesFraudulent, this.votesNonFraudulent);
	}

	public Set<Feature> getVotesFraudulent() {
		return this.votesFraudulent;
	}

	// compute and return the ensemble decision based on a required percentage
	public boolean getIsAnomalousWithMajority(Double isFraudulentThreshold) {
		int numFraudulent = this.votesFraudulent.size();
		int numTotal = this.votesNonFraudulent.size() + numFraudulent;
		return numFraudulent / numTotal >= isFraudulentThreshold;
	}

	// add all votes from a different EventStatistics to the internal state
	public EventStatistics withVotesFrom(EventStatistics eventStatistics) {
		Tuple2<Set<Feature>, Set<Feature>> incomingVotes = eventStatistics.getFeatureVotes();
		this.votesFraudulent.addAll(incomingVotes.f0);
		this.votesNonFraudulent.addAll(incomingVotes.f1);
		return this;
	}
}
