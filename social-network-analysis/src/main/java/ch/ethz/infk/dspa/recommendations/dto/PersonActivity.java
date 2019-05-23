package ch.ethz.infk.dspa.recommendations.dto;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.recommendations.dto.Category.CategoryType;

public class PersonActivity {

	public enum PersonActivityType {
		POST, COMMENT, LIKE, STATIC
	}

	private PersonActivityType type;
	private Long personId;
	private Long postId;
	private Map<String, Integer> categoryMap;

	public PersonActivity() {
		this.categoryMap = new HashMap<>();
	}

	public PersonActivity(Long personId, Long postId, PersonActivityType type) {
		this.personId = personId;
		this.postId = postId;
		this.type = type;
		this.categoryMap = new HashMap<>();
	}

	public static PersonActivity ofStatic(Long personId, Map<String, Integer> map) {

		PersonActivity activity = new PersonActivity();
		activity.setPersonId(personId);
		activity.setType(PersonActivityType.STATIC);
		activity.setCategoryMap(map);
		return activity;

	}

	public static PersonActivity of(Post post) {

		Map<String, Integer> map = new HashMap<>();

		List<Long> tags = ObjectUtils.defaultIfNull(post.getTags(), Collections.emptyList());
		for (Long tag : tags) {
			String category = Category.tag(tag);
			Integer count = map.getOrDefault(category, 0);
			map.put(category, count + 1);
		}

		Long forumId = post.getForumId();
		if (forumId != null) {
			String category = Category.forum(forumId);
			Integer count = map.getOrDefault(category, 0);
			map.put(category, count + 1);
		}

		Long placeId = post.getPlaceId();
		if (placeId != null) {
			String category = Category.place(placeId);
			Integer count = map.getOrDefault(category, 0);
			map.put(category, count + 1);
		}

		String language = post.getLanguage();
		if (StringUtils.isNotEmpty(language)) {
			String category = Category.language(language);
			Integer count = map.getOrDefault(category, 0);
			map.put(category, count + 1);
		}

		PersonActivity activity = new PersonActivity();
		activity.setPostId(post.getId());
		activity.setPersonId(post.getPersonId());
		activity.setType(PersonActivityType.POST);
		activity.setCategoryMap(map);

		return activity;
	}

	public static PersonActivity of(Comment comment) {

		Map<String, Integer> map = new HashMap<>();

		Long placeId = comment.getPlaceId();
		if (placeId != null) {
			String category = Category.place(placeId);
			Integer count = map.getOrDefault(category, 0);
			map.put(category, count + 1);
		}

		PersonActivity activity = new PersonActivity();
		activity.setPostId(comment.getReplyToPostId());
		activity.setPersonId(comment.getPersonId());
		activity.setType(PersonActivityType.COMMENT);
		activity.setCategoryMap(map);

		return activity;
	}

	public static PersonActivity of(Like like) {
		PersonActivity activity = new PersonActivity();
		activity.setPostId(like.getPostId());
		activity.setPersonId(like.getPersonId());
		activity.setType(PersonActivityType.LIKE);

		return activity;
	}

	public Long getPersonId() {
		return this.personId;
	}

	public Long getPostId() {
		return this.postId;
	}

	public boolean onlyStatic() {
		return type != null && type == PersonActivityType.STATIC;
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

	public ImmutableMap<String, Integer> getCategoryMap() {
		return ImmutableMap.copyOf(this.categoryMap);
	}

	public void setCategoryMap(Map<String, Integer> categoryMap) {
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((categoryMap == null) ? 0 : categoryMap.hashCode());
		result = prime * result + ((personId == null) ? 0 : personId.hashCode());
		result = prime * result + ((postId == null) ? 0 : postId.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		PersonActivity other = (PersonActivity) obj;
		if (categoryMap == null) {
			if (other.categoryMap != null) return false;
		} else if (!categoryMap.equals(other.categoryMap)) return false;
		if (personId == null) {
			if (other.personId != null) return false;
		} else if (!personId.equals(other.personId)) return false;
		if (postId == null) {
			if (other.postId != null) return false;
		} else if (!postId.equals(other.postId)) return false;
		if (type != other.type) return false;
		return true;
	}

	@Override
	public String toString() {
		return "PersonActivity [type=" + type + ", personId=" + personId + ", postId=" + postId + ", categoryMap="
				+ categoryMap + "]";
	}

}
