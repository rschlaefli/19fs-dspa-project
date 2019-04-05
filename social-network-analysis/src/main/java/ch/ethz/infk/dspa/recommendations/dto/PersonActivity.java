package ch.ethz.infk.dspa.recommendations.dto;

import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple3;

public class PersonActivity extends Tuple3<Long, Long, HashMap<String, Integer>> {

	private static final long serialVersionUID = 1L;

	public Long personId() {
		return this.f0;
	}

	public Long postId() {
		return this.f1;
	}

	public HashMap<String, Integer> categoryMap() {
		return this.f2;
	}

	public void setPersonId(Long personId) {
		this.f0 = personId;
	}

	public void setPostId(Long postId) {
		this.f1 = postId;
	}

	public void countCategory(String category) {
		this.f2.merge(category, 1, Integer::sum);
	}

	public void putCategory(String category, Integer count) {
		this.f2.put(category, count);
	}

	public Integer count(String category) {
		return this.f2.getOrDefault(category, 0);
	}

}
