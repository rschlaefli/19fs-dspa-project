package ch.ethz.infk.dspa.recommendations.dto;

import java.util.HashMap;

public class PersonActivity {

	private Long personId;
	private Long postId;
	private HashMap<String, Integer> categoryMap;

	public PersonActivity() {
		categoryMap = new HashMap<String, Integer>();
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

}
