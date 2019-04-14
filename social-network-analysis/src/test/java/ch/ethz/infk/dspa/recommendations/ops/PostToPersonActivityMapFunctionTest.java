package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.recommendations.dto.Category;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;

public class PostToPersonActivityMapFunctionTest {

	@Test
	public void testPostToPersonActivityMapFunction() throws Exception {
		PostToPersonActivityMapFunction function = new PostToPersonActivityMapFunction();

		Post post = new PostTestDataGenerator().generateElement();
		PersonActivity personActivity = function.map(post);

		assertEquals(post.getId(), personActivity.postId(), "post id");
		assertEquals(post.getPersonId(), personActivity.personId(), "person id");

		// Check Category Map
		Map<String, Integer> categoryMap = personActivity.categoryMap();
		assertEquals(4, categoryMap.size(), "category map size");
		assertEquals(1, (int) categoryMap.get(Category.forum(post.getForumId())), "category forum");
		assertEquals(1, (int) categoryMap.get(Category.language(post.getLanguage())), "category language");
		assertEquals(1, (int) categoryMap.get(Category.tag(post.getTags().get(0))), "category tags");
		assertEquals(1, (int) categoryMap.get(Category.place((post.getPlaceId()))), "category place");
	}

}
