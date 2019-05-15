package ch.ethz.infk.dspa.recommendations.dto;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Category {

	public enum CategoryType {
		TAG, PLACE, FORUM, LANGUAGE, TAGCLASS, ORGANISATION
	}

	public static final String TAG = "tag_%d";
	public static final Pattern TAG_PATTERN = Pattern.compile("^tag_(\\d+)");
	public static final String PLACE = "place_%d";
	public static final Pattern PLACE_PATTERN = Pattern.compile("^place_(\\d+)");
	public static final String FORUM = "forum_%d";
	public static final Pattern FORUM_PATTERN = Pattern.compile("^forum_(\\d+)");
	public static final String LANGUAGE = "lan_%s";
	public static final Pattern LANGUAGE_PATTERN = Pattern.compile("^lan_(\\w+)");
	public static final String TAGCLASS = "tagclass_%d";
	public static final Pattern TAGCLASS_PATTERN = Pattern.compile("^tagclass_(\\d+)");
	public static final String ORGANISATION = "org_%d";
	public static final Pattern ORGANISATION_PATTERN = Pattern.compile("^org_(\\d+)");

	@SuppressWarnings("unchecked")
	public static <T> T getId(CategoryType category, String str) {
		switch (category) {
		case FORUM:
			return (T) getForum(str);
		case LANGUAGE:
			return (T) getLanguage(str);
		case PLACE:
			return (T) getPlace(str);
		case TAG:
			return (T) getTag(str);
		case TAGCLASS:
			return (T) getTagClass(str);
		case ORGANISATION:
			return (T) getOrganisation(str);
		default:
			throw new IllegalArgumentException("Unknown CategoryType");
		}
	}

	public static boolean isCategory(CategoryType category, String str) {
		switch (category) {
		case FORUM:
			return isForum(str);
		case LANGUAGE:
			return isLanguage(str);
		case PLACE:
			return isPlace(str);
		case TAG:
			return isTag(str);
		case TAGCLASS:
			return isTagClass(str);
		case ORGANISATION:
			return isOrganisation(str);
		default:
			return false;
		}
	}

	public static String tag(Long tagId) {
		return String.format(TAG, tagId);
	}

	public static boolean isTag(String str) {
		return TAG_PATTERN.matcher(str).matches();
	}

	public static Long getTag(String str) {
		Matcher m = TAG_PATTERN.matcher(str);
		m.matches();
		return Long.parseLong(m.group(1));
	}

	public static String place(Long placeId) {
		return String.format(PLACE, placeId);
	}

	public static boolean isPlace(String str) {
		return PLACE_PATTERN.matcher(str).matches();
	}

	public static Long getPlace(String str) {
		Matcher m = PLACE_PATTERN.matcher(str);
		m.matches();
		return Long.parseLong(m.group(1));
	}

	public static String forum(Long forumId) {
		return String.format(FORUM, forumId);
	}

	public static boolean isForum(String str) {
		return FORUM_PATTERN.matcher(str).matches();
	}

	public static Long getForum(String str) {
		Matcher m = FORUM_PATTERN.matcher(str);
		m.matches();
		return Long.parseLong(m.group(1));
	}

	public static String language(String language) {
		return String.format(LANGUAGE, language);
	}

	public static boolean isLanguage(String str) {
		return LANGUAGE_PATTERN.matcher(str).matches();
	}

	public static String getLanguage(String str) {
		Matcher m = LANGUAGE_PATTERN.matcher(str);
		m.matches();
		return m.group(1);
	}

	public static String tagclass(Long tagClass) {
		return String.format(TAGCLASS, tagClass);
	}

	public static boolean isTagClass(String str) {
		return TAGCLASS_PATTERN.matcher(str).matches();
	}

	public static Long getTagClass(String str) {
		Matcher m = TAGCLASS_PATTERN.matcher(str);
		m.matches();
		return Long.parseLong(m.group(1));
	}

	public static String organisation(Long organisationId) {
		return String.format(ORGANISATION, organisationId);
	}

	public static boolean isOrganisation(String str) {
		return ORGANISATION_PATTERN.matcher(str).matches();
	}

	public static Long getOrganisation(String str) {
		Matcher m = ORGANISATION_PATTERN.matcher(str);
		m.matches();
		return Long.parseLong(m.group(1));
	}

}
