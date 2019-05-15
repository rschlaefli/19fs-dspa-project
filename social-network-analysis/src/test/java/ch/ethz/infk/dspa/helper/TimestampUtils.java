package ch.ethz.infk.dspa.helper;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

import org.apache.flink.streaming.api.windowing.time.Time;

public class TimestampUtils {

	public static String formatWindow(long windowStart, long windowEnd) {
		return "Window: " + formatTimestamp(windowStart) + " - " + formatTimestamp(windowEnd);
	}

	public static String formatTimestamp(long timestamp) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

		Date dateTime = Date.from(Instant.ofEpochMilli(timestamp - Time.hours(1).toMilliseconds()));

		return format.format(dateTime);
	}

}
