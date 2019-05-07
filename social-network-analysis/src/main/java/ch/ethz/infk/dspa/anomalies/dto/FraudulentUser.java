package ch.ethz.infk.dspa.anomalies.dto;

import org.apache.flink.api.java.tuple.Tuple2;

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
}
