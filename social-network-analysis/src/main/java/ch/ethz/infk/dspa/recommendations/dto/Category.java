package ch.ethz.infk.dspa.recommendations.dto;

public class Category {

	public static final String TAG = "tag_%d";
	public static final String PLACE = "place_%d";
	public static final String FORUM = "forum_%d";
	public static final String LANGUAGE = "lan_%s";

	public static String tag(Long tagId) {
		return String.format(TAG, tagId);
	}

	public static String place(Long placeId) {
		return String.format(PLACE, placeId);
	}

	public static String forum(Long forumId) {
		return String.format(FORUM, forumId);
	}

	public static String language(String language) {
		return String.format(LANGUAGE, language);
	}

}
