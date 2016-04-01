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

package uk.ac.ox.it.ords.api.database.structure.resources;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import uk.ac.ox.it.ords.api.database.structure.model.OrdsDB;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.HibernateUtils;

public class AbstractDatabaseTestRunner extends AbstractDatabaseTest {
	
	static int logicalDatabaseId;
	
	@BeforeClass
	public static void setup(){
		Session session = HibernateUtils.getSessionFactory().getCurrentSession();
		Transaction transaction = session.beginTransaction();
		OrdsDB database = new OrdsDB();
		database.setDbName("DatabaseTest");
		database.setDbDescription("DatabaseTest");
		database.setDatabaseType("testing");
		session.save(database);
		transaction.commit();
		logicalDatabaseId = database.getLogicalDatabaseId();
	}
	
	@AfterClass
	public static void tearDown(){
		Session session = HibernateUtils.getSessionFactory().getCurrentSession();
		Transaction transaction = session.beginTransaction();
		OrdsDB database = new OrdsDB();
		database.setLogicalDatabaseId(logicalDatabaseId);
		session.delete(database);
		transaction.commit();
	}

}
