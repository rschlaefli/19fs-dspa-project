package ch.ethz.infk.dspa.anomalies.ops.feature;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import ch.ethz.infk.dspa.anomalies.ops.features.ContentsFeatureMapFunction;
import ch.ethz.infk.dspa.avro.Comment;
import ch.ethz.infk.dspa.avro.Post;
import ch.ethz.infk.dspa.stream.testdata.CommentTestDataGenerator;
import ch.ethz.infk.dspa.stream.testdata.PostTestDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ContentsFeatureMapFunctionTest {
	@ParameterizedTest
	@CsvSource(value = {
			"hi there | CONTENTS_SHORT | 2",
			"hello world | CONTENTS_SHORT | 2",
			"today is a great day | CONTENTS_MEDIUM | 5",
			"Hello there, today is a great day. Do you think it is as great as it sounds? Why would you not, it is as good as it can be. | CONTENTS_LONG | 22"
	}, delimiter = '|')
	void testContentsFeatureMapFunction(String content, String featureId, double uniqueStems)
			throws Exception {
		MapFunction<Feature, Feature> mapper = new ContentsFeatureMapFunction(4, 15);

		Post post = new PostTestDataGenerator().generateElement();
		post.setContent(content);
		Feature postFeature = Feature.of(post);
		Feature mappedPostFeature = mapper.map(postFeature);

		Comment comment = new CommentTestDataGenerator().generateElement();
		comment.setContent(content);
		Feature commentFeature = Feature.of(comment);
		Feature mappedCommentFeature = mapper.map(commentFeature);

		assertEquals(featureId, mappedPostFeature.getFeatureId().name(), String
				.format("FeatureIds do not match for Post: %s - %s", featureId, mappedPostFeature.getFeatureId()));
		assertEquals(featureId, mappedCommentFeature.getFeatureId().name(), String
				.format("FeatureIds do not match for Comment: %s - %s", featureId,
						mappedCommentFeature.getFeatureId()));

		assertEquals(uniqueStems, mappedPostFeature.getFeatureValue().doubleValue(), String.format(
				"Number of stems does not match for Post: %f - %f", uniqueStems, mappedPostFeature.getFeatureValue()));
		assertEquals(uniqueStems, mappedCommentFeature.getFeatureValue().doubleValue(), String.format(
				"Number of stems does not match for Comment: %f - %f", uniqueStems,
				mappedCommentFeature.getFeatureValue()));
	}
}
