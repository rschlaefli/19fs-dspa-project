package ch.ethz.infk.dspa.anomalies.dto;

import com.google.common.base.Objects;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FraudulentUser {

	public Long personId;
	public Set<String> anomalousEvents;
	public Map<Feature.FeatureId, Integer> voteCounts;

	public FraudulentUser(Long personId) {
		this.personId = personId;
		this.anomalousEvents = new HashSet<>();
		this.voteCounts = new HashMap<>();
	}

	public FraudulentUser withVotesFrom(EventStatistics eventStatistics) {
		this.anomalousEvents.add(eventStatistics.getEventGUID());

		Set<Feature> votesFraudulent = eventStatistics.getVotesFraudulent();
		for (Feature feature : votesFraudulent) {
			this.voteCounts.compute(feature.getFeatureId(), (k, v) -> (v == null) ? 1 : v + 1);
		}

		return this;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		FraudulentUser that = (FraudulentUser) o;
		return Objects.equal(personId, that.personId) &&
				Objects.equal(anomalousEvents, that.anomalousEvents) &&
				Objects.equal(voteCounts, that.voteCounts);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(personId, anomalousEvents, voteCounts);
	}

	@Override
	public String toString() {
		return "FraudulentUser{" +
				"personId=" + personId +
				", anomalousEvents=" + anomalousEvents +
				", voteCounts=" + voteCounts +
				'}';
	}
}
