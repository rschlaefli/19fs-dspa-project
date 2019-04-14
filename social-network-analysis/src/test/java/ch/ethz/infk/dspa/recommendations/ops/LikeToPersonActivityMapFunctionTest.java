package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.stream.testdata.LikeTestDataGenerator;

public class LikeToPersonActivityMapFunctionTest {

	@Test
	public void testLikeToPersonActivityMapFunction() throws Exception {
		LikeToPersonActivityMapFunction function = new LikeToPersonActivityMapFunction();

		Like like = new LikeTestDataGenerator().generateElement();
		PersonActivity personActivity = function.map(like);

		assertEquals(like.getPostId(), personActivity.postId(), "post id");
		assertEquals(like.getPersonId(), personActivity.personId(), "person id");

		// Check Category Map
		Map<String, Integer> categoryMap = personActivity.categoryMap();
		assertEquals(0, categoryMap.size(), "category map size");
	}

}
