package ch.ethz.infk.dspa.recommendations.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.StaticCategoryMap;
import ch.ethz.infk.dspa.stream.helper.TestSink;
import ch.ethz.infk.dspa.stream.testdata.Tuple0TestDataGenerator;

public class StaticPersonActivityOutputProcessFunctionIT extends AbstractTestBase {

	private StreamExecutionEnvironment env;
	private Set<Long> testDataTimestamps;
	private DataStream<Tuple0> windowActivateStream;

	@BeforeEach
	public void setup() throws IOException {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Tuple0TestDataGenerator generator = new Tuple0TestDataGenerator();
		this.windowActivateStream = generator.generate(this.env,
				"src/test/java/resources/recommendations/streams/tuple0_stream.csv",
				Time.hours(1));

		this.testDataTimestamps = generator.getTestData().stream().map(x -> x.getTimestamp().getMillis())
				.collect(Collectors.toSet());

		TestSink.reset();
	}

	@Test
	public void testStaticPersonActivityOutputProcessFunctionIT() throws Exception {
		Time windowSize = Time.hours(4);

		final String staticFilePath = "src/test/java/resources/recommendations/relations/";
		String personSpeaksRelationFile = staticFilePath + "person_speaks_language.csv";
		String personInterestRelationFile = staticFilePath + "person_hasInterest_tag.csv";
		String personLocationRelationFile = staticFilePath + "person_isLocatedIn_place.csv";
		String personWorkplaceRelationFile = staticFilePath + "person_workAt_organisation.csv";
		String personStudyRelationFile = staticFilePath + "person_studyAt_organisation.csv";

		windowActivateStream.keyBy(x -> 0L)
				.process(new StaticPersonActivityOutputProcessFunction(windowSize, personSpeaksRelationFile,
						personInterestRelationFile, personLocationRelationFile, personWorkplaceRelationFile,
						personStudyRelationFile))
				.setParallelism(1)
				.addSink(new TestSink<PersonActivity>());

		env.execute();

		Map<Long, List<PersonActivity>> results = TestSink.getResultsInTumblingWindow(PersonActivity.class, windowSize);

		StaticCategoryMap staticCategoryMap = new StaticCategoryMap()
				.withPersonInterestRelation(personInterestRelationFile)
				.withPersonLocationRelation(personLocationRelationFile)
				.withPersonSpeaksRelation(personSpeaksRelationFile)
				.withPersonStudyWorkAtRelations(personStudyRelationFile, personWorkplaceRelationFile);
		Set<PersonActivity> expectedPersonActivities = new HashSet<>(staticCategoryMap.getPersonActivities());

		Set<Long> expectedWindows = testDataTimestamps.stream()
				.map(x -> TimeWindow.getWindowStartWithOffset(x, 0, windowSize.toMilliseconds()))
				.collect(Collectors.toSet());

		// check that in every window all static person activities are present
		for (List<PersonActivity> activities : results.values()) {
			Set<PersonActivity> activitySet = new HashSet<>(activities);
			assertEquals(expectedPersonActivities.size(), activities.size(),
					"number of static activities does not match");
			assertEquals(expectedPersonActivities, activitySet, "static person activities do not match");
		}

		// check that windows are as expected
		assertEquals(expectedWindows, results.keySet(), "windows do not match");
	}

}
