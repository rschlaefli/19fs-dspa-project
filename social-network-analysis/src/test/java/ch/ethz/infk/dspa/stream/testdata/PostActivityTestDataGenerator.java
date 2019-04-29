package ch.ethz.infk.dspa.stream.testdata;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.statistics.dto.PostActivity;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;

public class PostActivityTestDataGenerator extends AbstractTestDataGenerator<PostActivity> {

    @Override
    public DataStream<PostActivity> addReturnType(SingleOutputStreamOperator<PostActivity> out) {
        return out.returns(PostActivity.class);
    }

    @Override
    public AssignerWithPeriodicWatermarks<TestDataPair<PostActivity>> getTimestampsAndWatermarkAssigner(
            Time maxOutOfOrderness) {
        return new BoundedOutOfOrdernessTimestampExtractor<TestDataPair<PostActivity>>(maxOutOfOrderness) {
            private static final long serialVersionUID = 1L;

            @Override
            public long extractTimestamp(TestDataPair<PostActivity> pair) {
                return pair.timestamp.getMillis();
            }
        };
    }

    @Override
    public PostActivity generateElement() {
        Long postId = 1L;
        Long personId = 2L;
        return new PostActivity(postId, PostActivity.ActivityType.POST, personId);
    }

    @Override
    public TestDataPair<PostActivity> parseLine(String line) {
        String[] parts = line.split("\\|");

        Long postId = Long.parseLong(parts[0]);
        PostActivity.ActivityType activityType = PostActivity.ActivityType.valueOf(parts[1]);
        Long personId = Long.parseLong(parts[2]);
        PostActivity activity = new PostActivity(postId, activityType, personId);

        DateTime creationDate = new DateTime(ZonedDateTime.parse(parts[3]).toInstant().toEpochMilli());

        return TestDataPair.of(activity, creationDate);
    }
}
