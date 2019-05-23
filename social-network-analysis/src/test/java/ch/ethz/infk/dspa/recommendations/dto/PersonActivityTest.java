package ch.ethz.infk.dspa.recommendations.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.LikeTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;

public class PersonActivityTest {

	@Test
	public void testCommentToPersonActivityMapFunction() throws Exception {
		Comment comment = new CommentTestDataGenerator().generateElement();
		PersonActivity personActivity = PersonActivity.of(comment);

		assertEquals(comment.getReplyToPostId(), personActivity.getPostId(), "post id");
		assertEquals(comment.getPersonId(), personActivity.getPersonId(), "person id");

		// Check Category Map
		Map<String, Integer> categoryMap = personActivity.getCategoryMap();
		assertEquals(1, categoryMap.size(), "category map size");
		assertEquals(1, (int) categoryMap.get(Category.place((comment.getPlaceId()))), "category place");
	}

	@Test
	public void testLikeToPersonActivityMapFunction() throws Exception {
		Like like = new LikeTestDataGenerator().generateElement();
		PersonActivity personActivity = PersonActivity.of(like);

		assertEquals(like.getPostId(), personActivity.getPostId(), "post id");
		assertEquals(like.getPersonId(), personActivity.getPersonId(), "person id");

		// Check Category Map
		Map<String, Integer> categoryMap = personActivity.getCategoryMap();
		assertEquals(0, categoryMap.size(), "category map size");
	}

	@Test
	public void testPostToPersonActivityMapFunction() throws Exception {
		Post post = new PostTestDataGenerator().generateElement();
		PersonActivity personActivity = PersonActivity.of(post);

		assertEquals(post.getId(), personActivity.getPostId(), "post id");
		assertEquals(post.getPersonId(), personActivity.getPersonId(), "person id");

		// Check Category Map
		Map<String, Integer> categoryMap = personActivity.getCategoryMap();
		assertEquals(4, categoryMap.size(), "category map size");
		assertEquals(1, (int) categoryMap.get(Category.forum(post.getForumId())), "category forum");
		assertEquals(1, (int) categoryMap.get(Category.language(post.getLanguage())), "category language");
		assertEquals(1, (int) categoryMap.get(Category.tag(post.getTags().get(0))), "category tags");
		assertEquals(1, (int) categoryMap.get(Category.place((post.getPlaceId()))), "category place");
	}

}
