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

import java.io.File;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.structure.model.OrdsDB;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.model.SchemaDesignerTable;
import uk.ac.ox.it.ords.api.database.structure.model.User;
import uk.ac.ox.it.ords.api.database.structure.services.ServerConfigurationService;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.model.UserRole;

public class HibernateUtils {
	Logger log = LoggerFactory.getLogger(HibernateUtils.class);

	private static SessionFactory sessionFactory;
	private static ServiceRegistry serviceRegistry;

	private static SessionFactory userDBSessionFactory;
	private static ServiceRegistry userDBServiceRegistry;

	private static String currentUser;
	
	protected static String HIBERNATE_CONFIGURATION_PROPERTY = "ords.hibernate.configuration";
	protected static String HIBERNATE_USER_CONFIGURATION_PROPERTY="ords.hibernate.user.configuration";

	protected static void addMappings(Configuration configuration){
		configuration.addAnnotatedClass(OrdsDB.class);
		configuration.addAnnotatedClass(OrdsPhysicalDatabase.class);
		configuration.addAnnotatedClass(SchemaDesignerTable.class);
		configuration.addAnnotatedClass(User.class);
		configuration.addAnnotatedClass(Permission.class);
		//configuration.addAnnotatedClass(Audit.class);
		configuration.addAnnotatedClass(UserRole.class);
	}
	
	private static void init() {
		try {
			Configuration configuration = new Configuration();
			String hibernateConfigLocation = MetaConfiguration.getConfiguration().getString(HIBERNATE_CONFIGURATION_PROPERTY);

			if (hibernateConfigLocation == null) {
				configuration.configure();
			} else {
				configuration.configure(new File(hibernateConfigLocation));
			}
			
			addMappings(configuration);

			serviceRegistry = new ServiceRegistryBuilder().applySettings(
					configuration.getProperties()).buildServiceRegistry();

			sessionFactory = configuration.buildSessionFactory(serviceRegistry);
		} catch (HibernateException he) {
			System.err.println("Error creating Session: " + he);
			throw new ExceptionInInitializerError(he);
		}
	}

	private static void initUserDBSessionFactory(String odbcUser,
			String odbcPassword, String databaseName) {
		try {
			Configuration userDBConfiguration = new Configuration();
			String hibernateConfigLocation = MetaConfiguration.getConfiguration().getString(HIBERNATE_USER_CONFIGURATION_PROPERTY);

			if (hibernateConfigLocation == null) {
				userDBConfiguration.configure();
			} else {
				userDBConfiguration.configure(hibernateConfigLocation);
			}
			userDBConfiguration.setProperty("hibernate.connection.url",
					"jdbc:postgresql://localhost/" + databaseName);
			userDBConfiguration.setProperty("hibernate.connection.username",
					odbcUser);
			userDBConfiguration.setProperty("hibernate.connection.password",
					odbcPassword);
			
			// oops don't do this or it adds all the ords tables to the user's database!
			//addMappings(userDBConfiguration);

			userDBServiceRegistry = new ServiceRegistryBuilder().applySettings(
					userDBConfiguration.getProperties()).buildServiceRegistry();

			userDBSessionFactory = userDBConfiguration
					.buildSessionFactory(userDBServiceRegistry);
		} catch (HibernateException he) {
			System.err.println("Error creating Session: " + he);
			throw new ExceptionInInitializerError(he);
		}
	}
	
	public static String getFirstDBServer ( ) throws Exception {
		ServerConfigurationService serverConf = ServerConfigurationService.Factory.getInstance();
		List<String> serverList = serverConf.getServers();
		if ( serverList.size() == 0 ) {
			throw new Exception("Configuration problem, no servers found in configuration files");
		}
		return serverList.get(0);

	}
	
	
	

	public static SessionFactory getUserDBSessionFactory(String databaseName,
			String username, String password) {
		if (!username.equals(currentUser)) {
			if (userDBSessionFactory != null) {
				userDBSessionFactory.close();
			}
			initUserDBSessionFactory(username, password, databaseName);
			currentUser = username;
		}
		return userDBSessionFactory;
	}
	
	
	

	public static SessionFactory getSessionFactory() {
		if (sessionFactory == null)
			init();
		return sessionFactory;
	}

	public static void closeSession() {
		sessionFactory.getCurrentSession().close();
	}
}
