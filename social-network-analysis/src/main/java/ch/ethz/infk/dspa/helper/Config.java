package ch.ethz.infk.dspa.helper;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.ConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;

public class Config {
	public static Configuration getConfig(String fileName) {
		Configurations configs = new Configurations();
		File propertiesFile = new File(fileName);

		try {
			Configuration config = configs.properties(propertiesFile);
			return config;
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static Configuration getConfig() {
		return getConfig("config.properties");
	}
}
