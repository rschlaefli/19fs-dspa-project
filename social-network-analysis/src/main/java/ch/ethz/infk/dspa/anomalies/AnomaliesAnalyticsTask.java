package ch.ethz.infk.dspa.anomalies;

import ch.ethz.infk.dspa.AbstractAnalyticsTask;
import ch.ethz.infk.dspa.anomalies.dto.Feature;

import ch.ethz.infk.dspa.anomalies.ops.TimestampFeatureMapFunction;
import ch.ethz.infk.dspa.avro.Post;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class AnomaliesAnalyticsTask extends AbstractAnalyticsTask<SingleOutputStreamOperator<Feature>, Feature> {

    @Override
    public AnomaliesAnalyticsTask initialize() throws Exception {
        this.withKafkaConsumerGroup("anomalies");
        super.initialize();
        return this;
    }

    @Override
    public AnomaliesAnalyticsTask build() {
        // map the input streams to separate features
        DataStream<Feature> postFeatureStream = this.postStream.map(Feature::of).returns(Feature.class);
        DataStream<Feature> commentFeatureStream = this.commentStream.map(Feature::of).returns(Feature.class);;
        DataStream<Feature> likeFeatureStream = this.likeStream.map(Feature::of).returns(Feature.class);;

        // combine all features into a single stream
        DataStream<Feature> featureStream = postFeatureStream.union(commentFeatureStream, likeFeatureStream);

        // TIMESTAMP
        SingleOutputStreamOperator<Feature> timestampFeatureStream = featureStream
                .keyBy(Feature::getPersonId)
                .map(new TimestampFeatureMapFunction());

        this.outputStream = timestampFeatureStream;

        // FeatureStreamBuilder.withStreams().withFeature(f1, ).withFeature(f2).
        // feature(new f1().with(likeStream, poatStream), new f2().with(...)).keyBy(f-> f.id)

        return this;
    }
}
