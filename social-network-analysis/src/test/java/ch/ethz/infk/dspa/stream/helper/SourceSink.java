package ch.ethz.infk.dspa.stream.helper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import ch.ethz.infk.dspa.avro.CommentPostMapping;

public class SourceSink implements SinkFunction<CommentPostMapping>, SourceFunction<CommentPostMapping> {

	private static final long serialVersionUID = 1L;

	private static BlockingQueue<CommentPostMapping> queue = new LinkedBlockingQueue<>();

	private volatile boolean isRunning = true;
	private Long numberOfElements;

	public SourceSink(Long numberOfElements) {
		this.numberOfElements = numberOfElements;
	}

	@Override
	public void run(SourceContext<CommentPostMapping> ctx) throws Exception {
		int count = 0;
		while (isRunning && count < numberOfElements) {
			count++;
			ctx.collect(queue.take());
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void invoke(CommentPostMapping mapping) throws Exception {
		queue.add(mapping);
	}

}
