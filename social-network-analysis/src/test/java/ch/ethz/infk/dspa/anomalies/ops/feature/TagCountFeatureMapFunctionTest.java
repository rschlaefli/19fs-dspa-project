package ch.ethz.infk.dspa.anomalies.ops.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.dto.Feature.FeatureId;
import ch.ethz.infk.dspa.anomalies.ops.features.TagCountFeatureMapFunction;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;

public class TagCountFeatureMapFunctionTest {

	public static final double DELTA = 0.000000000001;

	@Test
	public void testTagCountFeatureMapFunctionSingleTag() throws Exception {

		TagCountFeatureMapFunction mapper = new TagCountFeatureMapFunction();

		Post post = new PostTestDataGenerator().generateElement();
		post.setTags(Collections.singletonList(123L));

		Feature feature = Feature.of(post);

		Feature result = mapper.map(feature);

		assertEquals(result.getFeatureId(), FeatureId.TAG_COUNT, "feature id not set");
		assertEquals(result.getFeatureValue(), 1.0, DELTA, "wrong tag count");

	}

	@Test
	public void testTagCountFeatureMapFunctionNullTag() throws Exception {

		TagCountFeatureMapFunction mapper = new TagCountFeatureMapFunction();

		Post post = new PostTestDataGenerator().generateElement();
		post.setTags(null);

		Feature feature = Feature.of(post);

		Feature result = mapper.map(feature);

		assertEquals(result.getFeatureId(), FeatureId.TAG_COUNT, "feature id not set");
		assertEquals(result.getFeatureValue(), 0.0, DELTA, "wrong tag count");

	}

	@Test
	public void testTagCountFeatureMapFunctionMultipleTags() throws Exception {

		TagCountFeatureMapFunction mapper = new TagCountFeatureMapFunction();

		Post post = new PostTestDataGenerator().generateElement();
		List<Long> tags = Arrays.asList(new Long[] { 1L, 2L, 3L, 4L });
		post.setTags(tags);

		Feature feature = Feature.of(post);

		Feature result = mapper.map(feature);

		assertEquals(result.getFeatureId(), FeatureId.TAG_COUNT, "feature id not set");
		assertEquals(result.getFeatureValue(), 4.0, DELTA, "wrong tag count");

	}

}
