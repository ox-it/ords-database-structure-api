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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import javax.ws.rs.core.Response;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissionSets;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.HibernateUtils;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.model.UserRole;

public class OdbcTest extends AbstractDatabaseTest {

	@Test
	public void createOdbcRole() throws Exception{
		
		//
		// First create a database
		//
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, 99, "localhost");
		Response response = getClient().path("/0/MAIN").post(dbr);
		assertEquals(201, response.getStatus());
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		//
		// Add a table
		//
		int dbID = db.getPhysicalDatabaseId();
		
		// Create a table
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/false").post(null);
		assertEquals(201, response.getStatus());
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		// Create a column
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		logout();
		
		//
		// Add Pinga as a user for this database
		//
		this.addViewer("pinga@penguins.com", dbID);
		
		//
		// Now lets add an ODBC role - this time Pinga is logging in and asking for access
		//
		loginUsingSSO("pinga@penguins.com","");

		Response odbcResponse = getClient().path("/"+dbID+"/MAIN/odbc/").post(null);
		assertEquals(200, odbcResponse.getStatus());
		String password = odbcResponse.readEntity(String.class);
		System.out.println(password);
		
		//
		// So, do we now have rights for pinga? Easiest way is to see if we can connect and run a query. It won't
		// return anything as its an empty table, but we'll get an Exception if the role isn't permitted.
		//
		CachedRowSet results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), "pingapenguinscom_ords", password);
		assertFalse(results.first());
		
		//
		// This should fail as pinga should only have viewer access
		//
		try {
			results = runSQLStatement("insert into \"testTable\" values ('testColumn' = 'banana')", "localhost", calculateInstanceName(db, "MAIN"), "pingapenguinscom_ords", password);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			assertEquals("ERROR: permission denied for relation testTable", e.getMessage());
		}
		
		//
		// Now drop pinga
		//
		logout();
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		odbcResponse = getClient().path("/"+dbID+"/MAIN/odbc/pingapenguinscom_ords").delete();
		assertEquals(200, odbcResponse.getStatus());
		
		//
		// This should now fail as pinga is no longer allowed access
		//
		try {
			results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), "pingapenguinscom_ords", password);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			assertEquals("ERROR: permission denied for relation testTable", e.getMessage());
		}
		
		//
		// Now drop the DB
		//
		Response deleteResponse = getClient().path("/"+dbID+"/MAIN").delete();
		assertEquals(200, deleteResponse.getStatus());
	}
	
	protected CachedRowSet runSQLStatement(String statement, String server,
			String databaseName, String user, String password) throws Exception {
		Connection connection = null;
		Properties connectionProperties = new Properties();
		PreparedStatement preparedStatement = null;
		connectionProperties.put("user", user);
		connectionProperties.put("password", password);
		String connectionURL = "jdbc:postgresql://" + server + "/"
				+ databaseName;
		try {
			connection = DriverManager.getConnection(connectionURL,
					connectionProperties);
			
			preparedStatement = connection.prepareStatement(statement);

			if (statement.toLowerCase().startsWith("select")) {
				ResultSet result = preparedStatement.executeQuery();
				CachedRowSet rowSet = RowSetProvider.newFactory()
						.createCachedRowSet();
				rowSet.populate(result);
				return rowSet;
			} else {
				preparedStatement.execute();
				return null;
			}
		}
		finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}
	
	private String calculateInstanceName ( OrdsPhysicalDatabase db, String instance ){
		return 	(instance + "_" + db.getPhysicalDatabaseId() + "_" + db.getLogicalDatabaseId()).toLowerCase();
	}
	
	private void addViewer(String principal, int databaseid){

		Session session = HibernateUtils.getSessionFactory().getCurrentSession();
		Transaction transaction = session.beginTransaction();

		for (String permission : DatabaseStructurePermissionSets.getPermissionsForViewer(99)){
			Permission permissionObject = new Permission();
			permissionObject.setRole("viewer_"+databaseid);
			permissionObject.setPermission(permission);
			session.save(permissionObject);
		}
		
		UserRole pinga = new UserRole();
		pinga.setPrincipalName(principal);
		pinga.setRole("viewer_"+databaseid);
		session.save(pinga);
		
		transaction.commit();
	}
}
