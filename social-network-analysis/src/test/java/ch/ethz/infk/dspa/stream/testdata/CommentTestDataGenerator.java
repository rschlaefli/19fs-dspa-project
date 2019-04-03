package ch.ethz.infk.dspa.stream.testdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.joda.time.DateTime;

import ch.ethz.infk.dspa.avro.Comment;

public class CommentTestDataGenerator {

	private List<Comment> comments = new ArrayList<>();
	private Map<Long, Long> map = new HashMap<>();

	public CommentTestDataGenerator() {
		/**
		 * post p1 - comment c1_1 - reply r1_1_1 - reply r1_1_1_1 - reply r1_1_1_2 -
		 * reply r1_1_2 post p2 - comment c2_1 - reply r2_1_1 - reply r2_1_2 - comment
		 * c2_2
		 */

		Long id = 0l;
		Long post1 = 10l;
		Long post2 = 20l;

		final DateTime base = DateTime.now().minusMinutes(100);
		Iterator<DateTime> seqDateTime = IntStream.range(0, 100)
				.mapToObj(offset -> base.plusMinutes(offset))
				.iterator();

		Comment c1_1 = Comment.newBuilder()
				.setId(id)
				.setReplyToPostId(post1)
				.setCreationDate(seqDateTime.next())
				.setPersonId(0l)
				.build();
		map.put(id, post1);
		comments.add(c1_1);

		Comment r1_1_1 = Comment.newBuilder()
				.setId(++id)
				.setReplyToCommentId(c1_1.getId())
				.setCreationDate(seqDateTime.next())
				.setPersonId(0l)
				.build();
		map.put(id, post1);
		comments.add(r1_1_1);

		Comment r1_1_1_1 = Comment.newBuilder()
				.setId(++id)
				.setReplyToCommentId(r1_1_1.getId())
				.setCreationDate(seqDateTime.next())
				.setPersonId(0l)
				.build();
		map.put(id, post1);
		comments.add(r1_1_1_1);

		Comment r1_1_1_2 = Comment.newBuilder()
				.setId(++id)
				.setReplyToCommentId(r1_1_1.getId())
				.setCreationDate(seqDateTime.next())
				.setPersonId(0l)
				.build();
		map.put(id, post1);
		comments.add(r1_1_1_2);

		Comment r1_1_2 = Comment.newBuilder()
				.setId(++id)
				.setReplyToCommentId(c1_1.getId())
				.setCreationDate(seqDateTime.next())
				.setPersonId(0l)
				.build();
		map.put(id, post1);
		comments.add(r1_1_2);

		Comment c2_1 = Comment.newBuilder()
				.setId(++id)
				.setReplyToPostId(post2)
				.setCreationDate(seqDateTime.next())
				.setPersonId(0l)
				.build();
		map.put(id, post2);
		comments.add(c2_1);

		Comment r2_1_1 = Comment.newBuilder()
				.setId(++id)
				.setReplyToCommentId(c2_1.getId())
				.setCreationDate(seqDateTime.next())
				.setPersonId(0l)
				.build();
		map.put(id, post2);
		comments.add(r2_1_1);

		Comment r2_1_2 = Comment.newBuilder()
				.setId(++id)
				.setReplyToCommentId(c2_1.getId())
				.setCreationDate(seqDateTime.next())
				.setPersonId(0l)
				.build();
		map.put(id, post2);
		comments.add(r2_1_2);

		Comment c2_2 = Comment.newBuilder()
				.setId(++id)
				.setReplyToPostId(post2)
				.setCreationDate(seqDateTime.next())
				.setPersonId(0l)
				.build();
		map.put(id, post2);
		comments.add(c2_2);
	}

	public List<Comment> getComments() {
		return comments;
	}

	public Map<Long, Long> getMap() {
		return map;
	}

}
