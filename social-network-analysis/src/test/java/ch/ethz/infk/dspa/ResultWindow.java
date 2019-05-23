package ch.ethz.infk.dspa;

import java.util.List;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Like;
import ch.ethz.infk.dspa.avro.Post;

public class ResultWindow<T> extends TimeWindow {

	public ResultWindow(long start, long end) {
		super(start, end);
	}

	public static <T> ResultWindow<T> of(TimeWindow timeWindow) {
		return new ResultWindow<>(timeWindow.getStart(), timeWindow.getEnd());
	}

	private List<Post> posts;
	private List<Comment> comments;
	private List<Like> likes;

	private List<T> results;

	public List<Post> getPosts() {
		return posts;
	}

	public void setPosts(List<Post> posts) {
		this.posts = posts;
	}

	public List<Comment> getComments() {
		return comments;
	}

	public void setComments(List<Comment> comments) {
		this.comments = comments;
	}

	public List<Like> getLikes() {
		return likes;
	}

	public void setLikes(List<Like> likes) {
		this.likes = likes;
	}

	public List<T> getResults() {
		return results;
	}

	public void setResults(List<T> results) {
		this.results = results;
	}

	public String toStringDetail() {
		return "ResultWindow [posts=" + posts + ", comments=" + comments + ", likes=" + likes + ", results=" + results
				+ "]";
	}

}
