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

import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.structure.conf.MetaConfiguration;

public class HibernateUtils {
    Logger log = LoggerFactory.getLogger(HibernateUtils.class);

    private static SessionFactory sessionFactory;
    private static ServiceRegistry serviceRegistry;

    private static SessionFactory userDBSessionFactory;
    private static ServiceRegistry userDBServiceRegistry;

    private static void init() {
	try {
	    Configuration configuration = new Configuration();
	    String hibernateConfigLocation = MetaConfiguration
		    .getConfigurationLocation("hibernate");

	    if (hibernateConfigLocation == null) {
		configuration.configure();
	    } else {
		configuration.configure(hibernateConfigLocation);
	    }

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
	    String hibernateConfigLocation = MetaConfiguration
		    .getConfigurationLocation("hibernateUserDBs");

	    userDBConfiguration.setProperty("hibernate.connection.url",
		    "jdbc:postgresql://localhost/" + databaseName);
	    userDBConfiguration.setProperty("hibernate.connection.username",
		    odbcUser);
	    userDBConfiguration.setProperty("hibernate.connection.password",
		    odbcPassword);

	    if (hibernateConfigLocation == null) {
		userDBConfiguration.configure();
	    } else {
		userDBConfiguration.configure(hibernateConfigLocation);
	    }

	    userDBServiceRegistry = new ServiceRegistryBuilder().applySettings(
		    userDBConfiguration.getProperties()).buildServiceRegistry();

	    userDBSessionFactory = userDBConfiguration
		    .buildSessionFactory(userDBServiceRegistry);
	} catch (HibernateException he) {
	    System.err.println("Error creating Session: " + he);
	    throw new ExceptionInInitializerError(he);
	}
    }

    public static SessionFactory getUserDBSessionFactory(String databaseName,
	    String username, String password) {
	// TODO: check if current userDBSessionFactory is pointing to the
	// correct database and
	// only close and re-initialize if not
	// NOTE: because this is a static code there may be threading issues
	// here!

	userDBSessionFactory.close();
	initUserDBSessionFactory(username, password, databaseName);
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
