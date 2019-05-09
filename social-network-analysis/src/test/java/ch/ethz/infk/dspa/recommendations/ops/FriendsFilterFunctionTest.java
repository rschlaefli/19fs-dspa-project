package ch.ethz.infk.dspa.recommendations.ops;

import ch.ethz.infk.dspa.recommendations.dto.PersonSimilarity;
import ch.ethz.infk.dspa.stream.testdata.PersonSimilarityTestDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FriendsFilterFunctionTest {

    private List<PersonSimilarity> similarities;

    @BeforeEach
    void setup() throws IOException {
        similarities = new PersonSimilarityTestDataGenerator().generate("src/test/java/resources/recommendations/streams/person_similarity_stream.csv");
    }

    @Test
    void testFriendsFilterFunction_loadsExpectedKnowsRelation() throws Exception {
        FriendsFilterFunction filterFunction = new FriendsFilterFunction("src/test/java/resources/recommendations/relations/person_knows_person.csv");

        // assert that the static data is correctly loaded
        filterFunction.open(new Configuration());
        Map<Long, Set<Long>> actualKnowsRelation = filterFunction.getKnowsRelation();

        Set<Long> friendsWithP1 = actualKnowsRelation.get(1L);
        assertTrue(friendsWithP1.containsAll(Arrays.asList(2L, 3L, 4L, 7L)), "Friends of P1 do not match expected values.");

        Set<Long> friendsWithP2 = actualKnowsRelation.get(2L);
        assertTrue(friendsWithP2.containsAll(Arrays.asList(3L)), "Friends of P2 do not match expected values.");

        Set<Long> friendsWithP3 = actualKnowsRelation.get(3L);
        assertTrue(friendsWithP3.containsAll(Arrays.asList(5L)), "Friends of P3 do not match expected values.");

        Set<Long> friendsWithP4 = actualKnowsRelation.get(4L);
        assertTrue(friendsWithP4.containsAll(Arrays.asList(7L)), "Friends of P4 do not match expected values.");

        Set<Long> friendsWithP7 = actualKnowsRelation.get(7L);
        assertTrue(friendsWithP7.containsAll(Arrays.asList(9L)), "Friends of P7 do not match expected values.");
    }

    @Test
    void testFriendsFilterFunction_filtersAsExpected() throws Exception {
        FriendsFilterFunction filterFunction = new FriendsFilterFunction("src/test/java/resources/recommendations/relations/person_knows_person.csv");
        filterFunction.open(new Configuration());

        // assert that the correct similarities are returned or filtered out
        List<PersonSimilarity> expectedSimilarities = Arrays.asList(
                new PersonSimilarity(2L, 8L).withSimilarity(0.0),
                new PersonSimilarity(3L, 9L).withSimilarity(0.3),
                new PersonSimilarity(4L, 8L).withSimilarity(0.4),
                new PersonSimilarity(6L, 2L).withSimilarity(0.3),
                new PersonSimilarity(7L, 3L).withSimilarity(0.4),
                new PersonSimilarity(2L, 9L).withSimilarity(0.2)
        );
        List<PersonSimilarity> actualSimilarities = similarities
                .stream()
                .filter(filterFunction::filter)
                .collect(Collectors.toList());

        for (PersonSimilarity expectedSimilarity : expectedSimilarities) {
            assertTrue(actualSimilarities.remove(expectedSimilarity), "Expected similarity " + expectedSimilarity + " not present!");
        }

        assertTrue(actualSimilarities.isEmpty(), "Received more results than expected!");
    }
}
