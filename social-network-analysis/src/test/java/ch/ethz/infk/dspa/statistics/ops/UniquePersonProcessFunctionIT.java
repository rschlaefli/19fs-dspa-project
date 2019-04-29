package ch.ethz.infk.dspa.statistics.ops;

import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import ch.ethz.infk.dspa.statistics.ops.UniquePersonProcessFunction;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.PostActivityTestDataGenerator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class UniquePersonProcessFunctionIT {

    private StreamExecutionEnvironment env;
    private DataStream<PostActivity> postActivityStream;

    @BeforeEach
    void setup() throws IOException {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        postActivityStream = new PostActivityTestDataGenerator().generate(env, "./src/test/java/resources/post_activity_event_stream.csv", Time.hours(1));

        TestSink.reset();
    }

    @Test
    void testUniquePersonProcessFunction() {
        postActivityStream
                .keyBy(PostActivity::getPostId)
                .process(new UniquePersonProcessFunction())
                .addSink(new TestSink<>());

        try {
            env.execute();
        } catch (Exception e) {
            fail("Failure in Flink Topology");
        }

        List<Tuple3<Long, Long, Integer>> expectedResults = Arrays.asList(
                Tuple3.of(0L, 1339747200000L, 1),
                Tuple3.of(0L, 1339750800000L, 2),
                Tuple3.of(0L, 1339754400000L, 3),
                Tuple3.of(0L, 1339758000000L, 3),
                Tuple3.of(0L, 1339761600000L, 3),
                Tuple3.of(0L, 1339765200000L, 3),
                Tuple3.of(0L, 1339768800000L, 3),
                Tuple3.of(0L, 1339772400000L, 3),
                Tuple3.of(0L, 1339776000000L, 3),
                Tuple3.of(0L, 1339779600000L, 3),
                Tuple3.of(0L, 1339783200000L, 3),
                Tuple3.of(0L, 1339786800000L, 3),
                Tuple3.of(0L, 1339790400000L, 2),
                Tuple3.of(1L, 1339758000000L, 1),
                Tuple3.of(1L, 1339761600000L, 2),
                Tuple3.of(1L, 1339765200000L, 2),
                Tuple3.of(1L, 1339768800000L, 2),
                Tuple3.of(1L, 1339772400000L, 2),
                Tuple3.of(1L, 1339776000000L, 2),
                Tuple3.of(1L, 1339779600000L, 2),
                Tuple3.of(1L, 1339783200000L, 2),
                Tuple3.of(1L, 1339786800000L, 2),
                Tuple3.of(1L, 1339790400000L, 2),
                Tuple3.of(2L, 1339765200000L, 1),
                Tuple3.of(2L, 1339768800000L, 1),
                Tuple3.of(2L, 1339772400000L, 2),
                Tuple3.of(2L, 1339776000000L, 2),
                Tuple3.of(2L, 1339779600000L, 3),
                Tuple3.of(2L, 1339783200000L, 3),
                Tuple3.of(2L, 1339786800000L, 3),
                Tuple3.of(2L, 1339790400000L, 3),
                Tuple3.of(3L, 1339783200000L, 1),
                Tuple3.of(3L, 1339786800000L, 1),
                Tuple3.of(3L, 1339790400000L, 1),
                Tuple3.of(4L, 1339786800000L, 1),
                Tuple3.of(4L, 1339790400000L, 1)
        );

        List<Tuple3> actualResults = TestSink.getResults(Tuple3.class);

        for (Tuple3<Long, Long, Integer> expectedResult : expectedResults) {
            assertTrue(actualResults.remove(expectedResult), "Expected result " + expectedResult + " not present!");
        }

        assertTrue(actualResults.isEmpty(), "Received more results than expected!");
    }
}
