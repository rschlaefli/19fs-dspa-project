package ch.ethz.infk.dspa.recommendations.dto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.recommendations.dto.Category.CategoryType;

public class PersonActivity {

	public enum PersonActivityType {
		POST, COMMENT, LIKE
	}

	private PersonActivityType type;
	private Long personId;
	private Long postId;

	private HashMap<String, Integer> categoryMap;

	public PersonActivity() {
		this.categoryMap = new HashMap<String, Integer>();
	}

	public PersonActivity(Long personId, Long postId, PersonActivityType type) {
		this();
		this.personId = personId;
		this.postId = postId;
		this.type = type;
	}

	public static PersonActivity of(Post post) {
		PersonActivity activity = new PersonActivity();
		activity.setPostId(post.getId());
		activity.setPersonId(post.getPersonId());
		activity.setType(PersonActivityType.POST);

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
		activity.setType(PersonActivityType.COMMENT);

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
		activity.setType(PersonActivityType.LIKE);

		// TODO [nku] check if want to keep creationTime in PersonActivity

		return activity;
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

	public PersonActivityType getType() {
		return type;
	}

	public void setType(PersonActivityType type) {
		this.type = type;
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

	public void mergeCategoryMap(Map<String, Integer> other) {
		other.forEach((category, count) -> this.categoryMap.merge(category, count, Integer::sum));
	}

	public void setCategoryMap(HashMap<String, Integer> categoryMap) {
		this.categoryMap = categoryMap;
	}

	public List<String> getCategoryKeys(CategoryType type) {
		return this.categoryMap.keySet().stream().filter(key -> Category.isCategory(type, key))
				.collect(Collectors.toList());
	}

	public Map<String, Integer> getCategories(CategoryType type) {
		return this.categoryMap.entrySet().stream().filter(entry -> Category.isCategory(type, entry.getKey()))
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
	}

	public Long extractLongIdFromKeySet(CategoryType category) {
		return this.categoryMap.keySet().stream()
				.filter(key -> Category.isCategory(category, key))
				.map(key -> Category.getId(category, key))
				.map(Long.class::cast)
				.findFirst().orElse(null);

	}
}
