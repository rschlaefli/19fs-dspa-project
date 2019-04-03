package ch.ethz.infk.dspa.stream.helper;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import ch.ethz.infk.dspa.avro.Comment;

public class TestCollectSink implements SinkFunction<Comment> {

	private static final long serialVersionUID = 1L;

	// must be static
	public static final List<Comment> values = new ArrayList<>();

	@Override
	public synchronized void invoke(Comment value) throws Exception {
		values.add(value);
	}
}
