package ch.ethz.infk.dspa.anomalies.ops.features;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Post;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class ContentsFeatureProcessFunction extends KeyedProcessFunction<Long, Feature, Feature> {

    @Override
    public void processElement(Feature feature, Context ctx, Collector<Feature> out) throws Exception {

        // extract post or comment contents based on the event type
        String contents;
        if (feature.getEventType() == Feature.EventType.POST) {
            contents = ((Post) feature.getEvent()).getContent();
        } else if (feature.getEventType() == Feature.EventType.COMMENT) {
            contents = ((Comment) feature.getEvent()).getContent();
        } else {
            return;
        }

        // create different features with different ids and value formulas applied
        // depending on the length of the content
        Feature updatedFeature;
        if (contents.length() > 1000) {
            updatedFeature = feature
                    .withFeatureId(Feature.FeatureId.CONTENTS_LONG)
                    .withFeatureValue(0.5);
        } else if (contents.length() > 250) {
            updatedFeature = feature
                    .withFeatureId(Feature.FeatureId.CONTENTS_MEDIUM)
                    .withFeatureValue(0.5);
        } else {
            updatedFeature = feature
                    .withFeatureId(Feature.FeatureId.CONTENTS_SHORT)
                    .withFeatureValue(0.5);
        }

        out.collect(updatedFeature);
    }

    public static DataStream<Feature> applyTo(DataStream<Feature> postInputStream, DataStream<Feature> commentInputStream) {
        return postInputStream
                .union(commentInputStream)
                .keyBy(Feature::getPersonId)
                // .filter(Feature::hasEventTypeWithContents)
                .process(new ContentsFeatureProcessFunction());
    }
}
