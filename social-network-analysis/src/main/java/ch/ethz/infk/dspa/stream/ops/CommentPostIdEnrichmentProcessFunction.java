package ch.ethz.infk.dspa.stream.ops;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.joda.time.DateTime;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.CommentPostMapping;

public class CommentPostIdEnrichmentProcessFunction extends CoProcessFunction<Comment, CommentPostMapping, Comment> {

	private static final long serialVersionUID = 1L;

	public static final OutputTag<CommentPostMapping> MAPPING_TAG = new OutputTag<>("comment-post",
			TypeInformation.of(CommentPostMapping.class));

	private transient ValueState<Long> postId;
	private transient ListState<Comment> children;

	@Override
	public void processElement1(Comment comment, Context ctx, Collector<Comment> out) throws Exception {
		if (comment.getReplyToPostId() != null) {
			// top level comment
			out.collect(comment);
			updatePostId(comment.getReplyToPostId(), ctx, out);
		} else if (this.postId.value() != null) {

			Long postId = this.postId.value();

			// reply with known mapping to post
			comment.setReplyToPostId(postId);
			out.collect(comment);

			// update mapping
			outputMapping(comment.getId(), postId, comment.getCreationDate(), ctx);

		} else {
			// reply with unknown mapping to post
			// needs to be buffered to wait for parent
			this.children.add(comment);

			// TODO [nku] register timer to delete once watermark passed

		}

	}

	@Override
	public void processElement2(CommentPostMapping mapping, Context ctx, Collector<Comment> out)
			throws Exception {
		updatePostId(mapping.getPostId(), ctx, out);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("postId", TypeInformation.of(Long.class));
		this.postId = getRuntimeContext().getState(descriptor);

		ListStateDescriptor<Comment> childrenDesc = new ListStateDescriptor<>("children", Comment.class);
		this.children = getRuntimeContext().getListState(childrenDesc);
	}

	private void updatePostId(Long postId, Context ctx, Collector<Comment> out) throws Exception {
		this.postId.update(postId);

		// output all children waiting for mapping
		for (Comment child : this.children.get()) {
			// output all children
			child.setReplyToPostId(postId);
			out.collect(child);

			// update mapping
			outputMapping(child.getId(), postId, child.getCreationDate(), ctx);
		}
		children.clear();

	}

	private void outputMapping(Long commentId, Long postId, DateTime eventTime, Context ctx) {

		CommentPostMapping mapping = CommentPostMapping.newBuilder()
				.setCommentId(commentId)
				.setPostId(postId)
				.setCommentCreationDate(eventTime)
				.build();

		ctx.output(MAPPING_TAG, mapping);
	}

}
