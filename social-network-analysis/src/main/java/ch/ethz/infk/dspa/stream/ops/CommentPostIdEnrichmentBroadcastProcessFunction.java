package ch.ethz.infk.dspa.stream.ops;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.stream.dto.CommentPostMapping;

public class CommentPostIdEnrichmentBroadcastProcessFunction
		extends KeyedBroadcastProcessFunction<Long, Comment, CommentPostMapping, Comment> {

	private static final long serialVersionUID = 1L;

	public static final MapStateDescriptor<Long, Long> COMMENT_POST_MAPPING_DESCRIPTOR = new MapStateDescriptor<>(
			"commentstream-commentId-postId-mapping",
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO);

	public static final MapStateDescriptor<Long, List<Long>> BUFFER_DESCRIPTOR = new MapStateDescriptor<>(
			"commentstream-buffer", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<List<Long>>() {
			}));

	public static final MapStateDescriptor<Long, Set<Long>> POST_WINDOW_DESCRIPTOR = new MapStateDescriptor<>(
			"commentstream-post--window", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<Set<Long>>() {
			}));

	private ValueState<Comment> commentBuffer;

	long expiredWindowSize;

	public CommentPostIdEnrichmentBroadcastProcessFunction(Time expiredWindowSize) {
		this.expiredWindowSize = expiredWindowSize.toMilliseconds();
	}

	@Override
	public void processBroadcastElement(CommentPostMapping mapping, Context ctx, Collector<Comment> out)
			throws Exception {

		// stores mapping <commentId, postId>
		BroadcastState<Long, Long> commentPostMapping = ctx.getBroadcastState(COMMENT_POST_MAPPING_DESCRIPTOR);

		// buffers commentIds (children) waiting for their parent commentId to be resolved:
		// buffer=<commentId,List<childCommentId>>
		BroadcastState<Long, List<Long>> buffer = ctx.getBroadcastState(BUFFER_DESCRIPTOR);

		// stores all postIds having comments within a certain window (used for detecting expired state)
		// postWindow=<window, Set<PostId>>
		BroadcastState<Long, Set<Long>> postWindow = ctx.getBroadcastState(POST_WINDOW_DESCRIPTOR);

		// try resolving post Id
		Long postId = null;
		if (mapping.getParentPostId() != null) {
			postId = mapping.getParentPostId();
		} else if (commentPostMapping.contains(mapping.getParentCommentId())) {
			postId = commentPostMapping.get(mapping.getParentCommentId());
		}

		if (postId != null) {
			// postId found -> add mapping and resolve all children + register interaction with post in window

			// add own mapping and recursively add mappings of buffered children
			resolve(mapping.getCommentId(), postId, buffer, commentPostMapping);

			// register interaction with post in window
			long expiredWindowStart = TimeWindow.getWindowStartWithOffset(ctx.timestamp(), 0, expiredWindowSize);

			Set<Long> postIds = ObjectUtils.defaultIfNull(postWindow.get(expiredWindowStart), new HashSet<>());
			postIds.add(postId);
			postWindow.put(expiredWindowStart, postIds);

		} else { // postId not found -> wait for parent to be resolved (add to buffer)
			Long parentCommentId = mapping.getParentCommentId();
			List<Long> waitingCommentIds = ObjectUtils.defaultIfNull(
					buffer.get(parentCommentId),
					new ArrayList<Long>());
			waitingCommentIds.add(mapping.getCommentId());
			buffer.put(parentCommentId, waitingCommentIds);
		}

		// clean up comment post mapping if watermark advanced far enough
		long currentWatermarkWindow = TimeWindow.getWindowStartWithOffset(ctx.currentWatermark(), 0, expiredWindowSize);
		long minWindow = getMinWindow(postWindow);

		if (minWindow + expiredWindowSize + expiredWindowSize < currentWatermarkWindow) {
			cleanupCommentPostMapping(commentPostMapping, postWindow, minWindow);
		}

	}

	private long getMinWindow(BroadcastState<Long, Set<Long>> postWindow) throws Exception {
		long minWindow = Long.MAX_VALUE;

		Iterator<Entry<Long, Set<Long>>> itr = postWindow.iterator();
		while (itr.hasNext()) {
			Entry<Long, Set<Long>> e = itr.next();
			long window = e.getKey();
			if (window < minWindow) {
				minWindow = window;
			}
		}

		return minWindow;
	}

	// TODO [nku] test the cleanup
	private void cleanupCommentPostMapping(BroadcastState<Long, Long> commentPostMapping,
			BroadcastState<Long, Set<Long>> postAccess, long minWindow)
			throws Exception {

		long nextWindow = minWindow + expiredWindowSize;

		Set<Long> expiredPostIds = postAccess.get(minWindow);
		postAccess.remove(minWindow);

		Set<Long> postIds2 = ObjectUtils.defaultIfNull(postAccess.get(nextWindow), Collections.emptySet());

		// remove all post ids also appearing in consecutive window
		expiredPostIds.removeAll(postIds2);

		// iterate over all mappings and remove expired ones
		Iterator<Entry<Long, Long>> itr = commentPostMapping.iterator();
		while (itr.hasNext()) {
			Entry<Long, Long> entry = itr.next();
			Long postId = entry.getValue();

			if (expiredPostIds.contains(postId)) {
				itr.remove();
			}
		}
	}

	private void resolve(Long commentId, Long postId, BroadcastState<Long, List<Long>> buffer,
			BroadcastState<Long, Long> commentPostMapping) throws Exception {

		commentPostMapping.put(commentId, postId);

		if (buffer.contains(commentId)) {
			List<Long> childCommentIds = buffer.get(commentId);
			buffer.remove(commentId);

			for (Long childId : childCommentIds) { // recursively resolve buffered
				resolve(childId, postId, buffer, commentPostMapping);
			}
		}

	}

	@Override
	public void processElement(Comment comment, ReadOnlyContext ctx,
			Collector<Comment> out) throws Exception {

		if (comment.getReplyToPostId() != null) {
			out.collect(comment);
			return;
		}

		ReadOnlyBroadcastState<Long, Long> commentPostMapping = ctx.getBroadcastState(COMMENT_POST_MAPPING_DESCRIPTOR);

		if (commentPostMapping.contains(comment.getId())) {
			// self is in mapping state
			Long postId = commentPostMapping.get(comment.getId());
			comment.setReplyToPostId(postId);
			out.collect(comment);

		} else if (commentPostMapping.contains(comment.getReplyToCommentId())) {
			// parent is in mapping state
			Long postId = commentPostMapping.get(comment.getReplyToCommentId());
			comment.setReplyToPostId(postId);
			out.collect(comment);

		} else {
			// need to wait
			ctx.timerService().registerEventTimeTimer(ctx.timestamp());
			commentBuffer.update(comment);
		}

	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Comment> out) throws Exception {

		Comment comment = commentBuffer.value();
		commentBuffer.clear();
		ReadOnlyBroadcastState<Long, Long> commentPostMapping = ctx.getBroadcastState(COMMENT_POST_MAPPING_DESCRIPTOR);

		if (commentPostMapping.contains(comment.getId())) {
			// self is in mapping state
			Long postId = commentPostMapping.get(comment.getId());
			comment.setReplyToPostId(postId);
		} else if (commentPostMapping.contains(comment.getReplyToCommentId())) {
			// parent is in mapping state
			Long postId = commentPostMapping.get(comment.getReplyToCommentId());
			comment.setReplyToPostId(postId);
		} else {
			// do nothing
		}

		out.collect(comment);
	}

	@Override
	public void open(Configuration parameters) throws Exception {

		ValueStateDescriptor<Comment> descriptor = new ValueStateDescriptor<>("comment",
				TypeInformation.of(Comment.class));

		this.commentBuffer = getRuntimeContext().getState(descriptor);

	}
}
