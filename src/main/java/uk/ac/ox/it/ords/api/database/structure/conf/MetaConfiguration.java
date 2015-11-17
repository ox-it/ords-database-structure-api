package uk.ac.ox.it.ords.api.database.structure.conf;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local configuration manager
 */
public class MetaConfiguration {

	public static Logger log = LoggerFactory.getLogger(MetaConfiguration.class);

	private static XMLConfiguration config;
	
	/**
	 * Load meta-configuration
	 */
    private static void load(){
		try {
			config = new XMLConfiguration("config.xml");
		} catch (Exception e) {
			config = null;
			log.warn("No server configuration location set; using defaults");
		}
	}

    /**
     * Get the specified configuration
     * @param identifier
     * @return the configuration, or Null if there is no match
     */
	public static String getConfigurationLocation(String identifier){
		if (config == null) load();
		String location = config.getString(identifier);
		if (location == null){
			log.warn("No configuration location set for "+identifier+"; using defaults");
		}
		return location;
	}

}
