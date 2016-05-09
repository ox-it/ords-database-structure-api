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

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.model.SchemaDesignerTable;
import uk.ac.ox.it.ords.api.database.structure.model.User;
import uk.ac.ox.it.ords.security.SimplePersistentSession;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;
import uk.ac.ox.it.ords.security.model.DatabaseServer;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.model.UserRole;
import uk.ac.ox.it.ords.security.services.ServerConfigurationService;

public class HibernateUtils {
	Logger log = LoggerFactory.getLogger(HibernateUtils.class);

	private static SessionFactory sessionFactory;
	private static ServiceRegistry serviceRegistry;
	
	protected static String HIBERNATE_CONFIGURATION_PROPERTY = "ords.hibernate.configuration";

	protected static void addMappings(Configuration configuration){
		configuration.addAnnotatedClass(OrdsPhysicalDatabase.class);
		configuration.addAnnotatedClass(SchemaDesignerTable.class);
		configuration.addAnnotatedClass(User.class);
		configuration.addAnnotatedClass(Permission.class);
		//configuration.addAnnotatedClass(Audit.class);
		configuration.addAnnotatedClass(UserRole.class);
		configuration.addAnnotatedClass(SimplePersistentSession.class);
	}
	
	private static void init() {
		
		String hibernateConfigLocation;
		try {
			hibernateConfigLocation = MetaConfiguration.getConfiguration().getString(HIBERNATE_CONFIGURATION_PROPERTY);
		} catch (Exception e) {
			hibernateConfigLocation = null;
		}
		
		try {
			Configuration configuration = new Configuration();

			if (hibernateConfigLocation == null) {
				configuration.configure();
			} else {
				configuration.configure(new File(hibernateConfigLocation));
			}
			
			//
			// Add server connection details
			//
			DatabaseServer databaseServer = ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer();
			configuration.setProperty("hibernate.connection.url", databaseServer.getUrl());
			configuration.setProperty("hibernate.connection.username", databaseServer.getUsername());
			configuration.setProperty("hibernate.connection.password", databaseServer.getPassword());
			
			addMappings(configuration);

			serviceRegistry = new ServiceRegistryBuilder().applySettings(
					configuration.getProperties()).buildServiceRegistry();

			sessionFactory = configuration.buildSessionFactory(serviceRegistry);
		} catch (Exception he) {
			System.err.println("Error creating Session: " + he);
			throw new ExceptionInInitializerError(he);
		}
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
