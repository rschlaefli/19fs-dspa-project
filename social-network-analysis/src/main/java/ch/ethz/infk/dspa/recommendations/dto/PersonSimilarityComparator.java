package ch.ethz.infk.dspa.recommendations.dto;

import java.util.Comparator;

public class PersonSimilarityComparator implements Comparator<PersonSimilarity> {

	@Override
	public int compare(PersonSimilarity o1, PersonSimilarity o2) {
		// first criteria: similarity
		int c = o1.similarity().compareTo(o2.similarity());
		if (c == 0) {
			// second criteria: personId
			c = o1.person2Id().compareTo(o2.person2Id());
		}
		return c;
	}
}
