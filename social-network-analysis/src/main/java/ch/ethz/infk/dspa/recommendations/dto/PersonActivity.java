package ch.ethz.infk.dspa.recommendations.dto;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;

public class PersonActivity {

	private Long personId;
	private Long postId;
	private HashMap<String, Integer> categoryMap;

	public PersonActivity() {
		categoryMap = new HashMap<String, Integer>();
	}

	public PersonActivity(Long personId, Long postId) {
		this();
		this.personId = personId;
		this.postId = postId;
	}

	public Long personId() {
		return this.personId;
	}

	public Long postId() {
		return this.postId;
	}

	public HashMap<String, Integer> categoryMap() {
		return this.categoryMap;
	}

	public void setPersonId(Long personId) {
		this.personId = personId;
	}

	public void setPostId(Long postId) {
		this.postId = postId;
	}

	public void countCategory(String category) {
		this.categoryMap.merge(category, 1, Integer::sum);
	}

	public void putCategory(String category, Integer count) {
		this.categoryMap.put(category, count);
	}

	public int count(String category) {
		return this.categoryMap.getOrDefault(category, 0);
	}

	public void mergeCategoryMap(HashMap<String, Integer> other) {
		other.forEach((category, count) -> categoryMap.merge(category, count, Integer::sum));
	}

	public void setCategoryMap(HashMap<String, Integer> categoryMap) {
		this.categoryMap = categoryMap;
	}

	public static PersonActivity of(Post post) {
		PersonActivity activity = new PersonActivity();

		// set postId and personId
		activity.setPostId(post.getId());
		activity.setPersonId(post.getPersonId());

		// TODO [nku] check if want to keep creationTime in PersonActivity

		// set categories of post
		List<Long> tags = post.getTags();
		if (tags != null) {
			tags.forEach(tagId -> activity.countCategory(Category.tag(tagId)));
		}

		Long forumId = post.getForumId();
		if (forumId != null) {
			activity.countCategory(Category.forum(forumId));
		}

		Long placeId = post.getPlaceId();
		if (placeId != null) {
			activity.countCategory(Category.place(placeId));
		}

		String language = post.getLanguage();
		if (StringUtils.isNotEmpty(language)) {
			activity.countCategory(Category.language(language));
		}

		// TODO potentially add content topic extraction

		return activity;
	}

	public static PersonActivity of(Comment comment) {
		PersonActivity activity = new PersonActivity();
		activity.setPostId(comment.getReplyToPostId());
		activity.setPersonId(comment.getPersonId());

		// TODO [nku] check if want to keep creationTime in PersonActivity

		Long placeId = comment.getPlaceId();
		if (placeId != null) {
			activity.countCategory(Category.place(placeId));
		}

		// TODO potentially add content topic extraction

		return activity;
	}

	public static PersonActivity of(Like like) {
		PersonActivity activity = new PersonActivity();
		activity.setPostId(like.getPostId());
		activity.setPersonId(like.getPersonId());

		// TODO [nku] check if want to keep creationTime in PersonActivity

		return activity;
	}
}
