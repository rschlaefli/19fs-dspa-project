package ch.ethz.infk.dspa.anomalies.ops.features;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import ch.ethz.infk.dspa.anomalies.dto.Feature;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.stemmer.snowball.SnowballStemmer;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;

public class ContentsFeatureMapFunction implements MapFunction<Feature, Feature> {

	private static final long serialVersionUID = 1L;

	private final int maxLengthShort;
	private final int minLengthLong;

	public ContentsFeatureMapFunction(int maxLengthShort, int minLengthLong) {
		this.maxLengthShort = maxLengthShort;
		this.minLengthLong = minLengthLong;
	}

	public DataStream<Feature> applyTo(DataStream<Feature> postInputStream,
			DataStream<Feature> commentInputStream) {
		return postInputStream.union(commentInputStream).map(this);
	}

	@Override
	public Feature map(Feature feature) {

		// extract post or comment contents based on the event type
		String content;
		if (feature.getEventType() == Feature.EventType.POST) {
			content = feature.getPost().getContent();
		} else if (feature.getEventType() == Feature.EventType.COMMENT) {
			content = feature.getComment().getContent();
		} else {
			throw new IllegalArgumentException("CANNOT_ANALYZE_LIKES_FOR_CONTENT");
		}

		// setup a tokenizer and extract tokens from the string
		Tokenizer tokenizer = SimpleTokenizer.INSTANCE;
		Stream<String> tokens = Arrays.stream(tokenizer.tokenize(content));

		// setup a stemmer and apply it to the extracted tokens
		Stemmer stemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH);
		List<String> stemmedTokens = tokens
				.map(token -> stemmer.stem(token).toString())
				.collect(Collectors.toList());

		// decide what kind of feature will be output depending on the content length
		Feature.FeatureId featureId = Feature.FeatureId.CONTENTS_SHORT;
		if (stemmedTokens.size() >= this.minLengthLong) {
			featureId = Feature.FeatureId.CONTENTS_LONG;
		} else if (stemmedTokens.size() > this.maxLengthShort) {
			featureId = Feature.FeatureId.CONTENTS_MEDIUM;
		}

		// create a set of unique tokens
		Set<String> uniqueStemmedTokens = new HashSet<>(stemmedTokens);

		// calculate the number of unique word stems in the content
		double uniqueStems = (double) uniqueStemmedTokens.size();

		// return a new feature with the computed feature id and value
		return feature.withFeatureId(featureId).withFeatureValue(uniqueStems);
	}
}
