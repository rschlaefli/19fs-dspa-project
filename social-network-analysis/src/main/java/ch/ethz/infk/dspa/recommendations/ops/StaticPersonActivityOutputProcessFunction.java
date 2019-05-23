package ch.ethz.infk.dspa.recommendations.ops;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import ch.ethz.infk.dspa.recommendations.dto.PersonActivity;
import ch.ethz.infk.dspa.recommendations.dto.StaticCategoryMap;

public class StaticPersonActivityOutputProcessFunction extends KeyedProcessFunction<Long, Tuple0, PersonActivity> {

	private static final long serialVersionUID = 1L;

	private final long windowSize;
	private final String personInterestRelationFile;
	private final String personLocationRelationFile;
	private final String personSpeaksRelationFile;
	private final String personStudyRelationFile;
	private final String personWorkplaceRelationFile;

	private List<PersonActivity> staticPersonActivities;

	public StaticPersonActivityOutputProcessFunction(Time windowSize, String personSpeaksRelationFile,
			String personInterestRelationFile,
			String personLocationRelationFile, String personWorkplaceRelationFile, String personStudyRelationFile) {
		this.windowSize = windowSize.toMilliseconds();
		this.personSpeaksRelationFile = personSpeaksRelationFile;
		this.personInterestRelationFile = personInterestRelationFile;
		this.personLocationRelationFile = personLocationRelationFile;
		this.personWorkplaceRelationFile = personWorkplaceRelationFile;
		this.personStudyRelationFile = personStudyRelationFile;
	}

	@Override
	public void processElement(Tuple0 in, Context ctx, Collector<PersonActivity> out) throws Exception {
		long windowStart = TimeWindow.getWindowStartWithOffset(ctx.timestamp(), 0, this.windowSize);
		long windowEnd = windowStart + this.windowSize - 1;

		ctx.timerService().registerEventTimeTimer(windowEnd);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<PersonActivity> out) throws Exception {
		// on timer output all the static person activities at the end of the window
		for (PersonActivity staticPersonActivity : staticPersonActivities) {
			out.collect(staticPersonActivity);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		// build person activities from static relations files
		StaticCategoryMap staticCategoryMap = new StaticCategoryMap()
				.withPersonInterestRelation(personInterestRelationFile)
				.withPersonLocationRelation(personLocationRelationFile)
				.withPersonSpeaksRelation(personSpeaksRelationFile)
				.withPersonStudyWorkAtRelations(personStudyRelationFile, personWorkplaceRelationFile);

		this.staticPersonActivities = staticCategoryMap.getPersonActivities();

	}

}
