/*
 * Copyright 2015 University of Oxford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.structure.services.ServerConfigurationService;


public class ServerConfigurationServiceImpl implements ServerConfigurationService {

	Logger log = LoggerFactory.getLogger(ServerConfigurationServiceImpl.class);
	
	public static final String DEFAULT_SERVER_CONFIG_LOCATION = "serverConfig.xml";


	@Override
	public List<String> getServers() throws Exception {
		
		String serverConfigurationLocation = DEFAULT_SERVER_CONFIG_LOCATION;

		try {
			//
			// Load the meta-configuration file
			//
			XMLConfiguration config = new XMLConfiguration("config.xml");
			serverConfigurationLocation = config.getString("ords.server.configuration");
			if (serverConfigurationLocation == null){
				log.warn("No server configuration location set; using defaults");
				serverConfigurationLocation = DEFAULT_SERVER_CONFIG_LOCATION;
			}
		} catch (Exception e) {
			log.warn("No server configuration location set; using defaults");
			serverConfigurationLocation = DEFAULT_SERVER_CONFIG_LOCATION;
		}
		
		try {
			//
			// Load the configuration file
			//
			XMLConfiguration serverConfig = new XMLConfiguration(serverConfigurationLocation);

			//
			// Read the server list
			//
			String[] servers = serverConfig.getStringArray("serverList.server[@name]");
			return Arrays.asList(servers);
		} catch (Exception e) {
			log.error("Unable to find server configuration file in " + serverConfigurationLocation);
			throw new FileNotFoundException();
		}
		

	}
}
