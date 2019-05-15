package ch.ethz.infk.dspa.recommendations.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class CategoryTest {

	Long tag = 10L;
	Long place = 20L;
	Long forum = 30L;
	String language = "en";
	Long tagClass = 40L;
	Long organisation = 50L;

	String tagCategoryName = Category.tag(tag);
	String placeCategoryName = Category.place(place);
	String forumCategoryName = Category.forum(forum);
	String languageCategoryName = Category.language(language);
	String tagClassCategoryName = Category.tagclass(tagClass);
	String organisationCategoryName = Category.organisation(organisation);

	@Test
	public void testTagCategory() {
		assertEquals("tag_" + tag, tagCategoryName, "Conversion tag-> tagCategoryName failed");

		assertTrue(Category.isTag(tagCategoryName), "isTag Category");
		assertFalse(Category.isTag(placeCategoryName), "isTag Category: place");
		assertFalse(Category.isTag(forumCategoryName), "isTag Category: forum");
		assertFalse(Category.isTag(languageCategoryName), "isTag Category: language");
		assertFalse(Category.isTag(tagClassCategoryName), "isTag Category: tagClass");
		assertFalse(Category.isTag(organisationCategoryName), "isTag Category: organisation");

		assertEquals(tag, Category.getTag(tagCategoryName), "getTag failed");
	}

	@Test
	public void testPlaceCategory() {
		assertEquals("place_" + place, placeCategoryName, "Conversion place-> placedCategoryName failed");

		assertTrue(Category.isPlace(placeCategoryName), "isPlace Category");
		assertFalse(Category.isPlace(tagCategoryName), "isPlace Category: tag");
		assertFalse(Category.isPlace(forumCategoryName), "isPlace Category: forum");
		assertFalse(Category.isPlace(languageCategoryName), "isPlace Category: language");
		assertFalse(Category.isPlace(tagClassCategoryName), "isPlace Category: tagClass");
		assertFalse(Category.isPlace(organisationCategoryName), "isPlace Category: organisation");

		assertEquals(place, Category.getPlace(placeCategoryName), "getPlace failed");
	}

	@Test
	public void testForumCategory() {
		assertEquals("forum_" + forum, forumCategoryName, "Conversion forum-> fourmCategoryName failed");

		assertTrue(Category.isForum(forumCategoryName), "isForum Category");
		assertFalse(Category.isForum(tagCategoryName), "isForum Category: tag");
		assertFalse(Category.isForum(placeCategoryName), "isForum Category: place");
		assertFalse(Category.isForum(languageCategoryName), "isForum Category: language");
		assertFalse(Category.isForum(tagClassCategoryName), "isForum Category: tagClass");
		assertFalse(Category.isForum(organisationCategoryName), "isForum Category: organisation");

		assertEquals(forum, Category.getForum(forumCategoryName), "getForum failed");
	}

	@Test
	public void testLanguageCategory() {
		assertEquals("lan_" + language, languageCategoryName, "Conversion language-> languageCategoryName failed");

		assertTrue(Category.isLanguage(languageCategoryName), "isLanguage Category");
		assertFalse(Category.isLanguage(tagCategoryName), "isLanguage Category: tag");
		assertFalse(Category.isLanguage(placeCategoryName), "isLanguage Category: place");
		assertFalse(Category.isLanguage(forumCategoryName), "isLanguage Category: forum");
		assertFalse(Category.isLanguage(tagClassCategoryName), "isLanguage Category: tagClass");
		assertFalse(Category.isLanguage(organisationCategoryName), "isLanguage Category: organisation");

		assertEquals(language, Category.getLanguage(languageCategoryName), "getLanguage failed");
	}

	@Test
	public void testTagClassCategory() {
		assertEquals("tagclass_" + tagClass, tagClassCategoryName, "Conversion tagClass-> tagClassCategoryName failed");

		assertTrue(Category.isTagClass(tagClassCategoryName), "isTagClass Category");
		assertFalse(Category.isTagClass(tagCategoryName), "isTagClass Category: tag");
		assertFalse(Category.isTagClass(placeCategoryName), "isTagClass Category: place");
		assertFalse(Category.isTagClass(forumCategoryName), "isTagClass Category: forum");
		assertFalse(Category.isTagClass(languageCategoryName), "isTagClass Category: language");
		assertFalse(Category.isTagClass(organisationCategoryName), "isTagClass Category: organisation");

		assertEquals(tagClass, Category.getTagClass(tagClassCategoryName), "getTagClass failed");
	}

	@Test
	public void testOrganisationCategory() {
		assertEquals("org_" + organisation, organisationCategoryName,
				"Conversion organisation-> organisationCategoryName failed");

		assertTrue(Category.isOrganisation(organisationCategoryName), "isOrganisation Category: organisation");
		assertFalse(Category.isOrganisation(tagClassCategoryName), "isOrganisation Category");
		assertFalse(Category.isOrganisation(tagCategoryName), "isOrganisation Category: tag");
		assertFalse(Category.isOrganisation(placeCategoryName), "isOrganisation Category: place");
		assertFalse(Category.isOrganisation(forumCategoryName), "isOrganisation Category: forum");
		assertFalse(Category.isOrganisation(languageCategoryName), "isOrganisation Category: language");

		assertEquals(organisation, Category.getOrganisation(organisationCategoryName), "getOrganisation failed");
	}

}
