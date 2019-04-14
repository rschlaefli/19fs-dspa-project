package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.recommendations.dto.Category;
import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;

public class CommentToPersonActivityMapFunctionTest {

	@Test
	public void testCommentToPersonActivityMapFunction() throws Exception {
		CommentToPersonActivityMapFunction function = new CommentToPersonActivityMapFunction();

		Comment comment = new CommentTestDataGenerator().generateElement();
		PersonActivity personActivity = function.map(comment);

		assertEquals(comment.getReplyToPostId(), personActivity.postId(), "post id");
		assertEquals(comment.getPersonId(), personActivity.personId(), "person id");

		// Check Category Map
		Map<String, Integer> categoryMap = personActivity.categoryMap();
		assertEquals(1, categoryMap.size(), "category map size");
		assertEquals(1, (int) categoryMap.get(Category.place((comment.getPlaceId()))), "category place");
	}

}
