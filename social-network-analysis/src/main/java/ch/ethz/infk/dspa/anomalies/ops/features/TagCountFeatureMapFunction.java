package ch.ethz.infk.dspa.anomalies.ops.features;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.avro.Post;

public class TagCountFeatureMapFunction implements MapFunction<Feature, Feature> {

	private static final long serialVersionUID = 1L;

	@Override
	public Feature map(Feature feature) throws Exception {
		Post post = feature.getPost();

		if (post == null) {
			throw new IllegalArgumentException("Feature must have a Post");
		}

		List<Long> tags = ObjectUtils.defaultIfNull(post.getTags(), Collections.emptyList());
		double tagCount = tags.size();

		return feature
				.withFeatureId(Feature.FeatureId.TAG_COUNT)
				.withFeatureValue(tagCount);

	}

	public DataStream<Feature> applyTo(DataStream<Feature> postInputStream) {
		return postInputStream.map(new TagCountFeatureMapFunction());
	}

}
