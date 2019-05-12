package ch.ethz.infk.dspa.anomalies.ops.features;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infk.dspa.anomalies.dto.Feature;

public class ContentsFeatureProcessFunction extends KeyedProcessFunction<Long, Feature, Feature> {
	// TODO [rsc] use map function instead of process function
	private static final long serialVersionUID = 1L;

	@Override
	public void processElement(Feature feature, Context ctx, Collector<Feature> out) throws Exception {

		// extract post or comment contents based on the event type
		String contents;
		if (feature.getEventType() == Feature.EventType.POST) {
			contents = feature.getPost().getContent();
		} else if (feature.getEventType() == Feature.EventType.COMMENT) {
			contents = feature.getComment().getContent();
		} else {
			throw new IllegalArgumentException("Unknown Event Type");
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

	public static DataStream<Feature> applyTo(DataStream<Feature> postInputStream,
			DataStream<Feature> commentInputStream) {
		return postInputStream
				.union(commentInputStream)
				.keyBy(Feature::getPersonId) // TODO [rsc] why key by?
				// .filter(Feature::hasEventTypeWithContents)
				.process(new ContentsFeatureProcessFunction());
	}
}
