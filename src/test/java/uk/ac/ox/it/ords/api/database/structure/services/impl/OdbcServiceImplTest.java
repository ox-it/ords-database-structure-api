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

package uk.ac.ox.it.ords.api.database.structure.services.impl;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import javax.ws.rs.core.Response;

import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.resources.AbstractDatabaseTest;
import uk.ac.ox.it.ords.api.database.structure.services.OdbcService;

public class OdbcServiceImplTest extends AbstractDatabaseTest{
	
	@Test
	public void createDatabaseAndAddODBCUser() throws Exception{
		
		//
		// We have a database
		// 
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, 12, "localhost");
		Response response = getClient().path("/24/MAIN").post(dbr);
		assertEquals(201, response.getStatus());
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);

		//
		// Add a table
		//
		// Strip the id from the end of the path
		int dbID = db.getPhysicalDatabaseId();
		
		// Create a table
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/false").post(null);
		assertEquals(201, response.getStatus());
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		// Create a column
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		//
		// Now lets add an ODBC role
		//
		OdbcService service = OdbcService.Factory.getInstance();
		service.addOdbcUserToDatabase("pingu", "pingu", db, calculateInstanceName(db, "MAIN"));
		
		//
		// So, do we now have rights for pingu? Easiest way is to see if we can connect and run a query. It won't
		// return anything as its an empty table, but we'll get an Exception if the role isn't permitted.
		//
		CachedRowSet results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), "pingu", "pingu");
		assertFalse(results.first());
		
		//
		// Now drop pingu
		//
		service.removeOdbcUserFromDatabase("pingu", db, calculateInstanceName(db, "MAIN"));
		
		//
		// This should now fail as pingu is no longer allowed access
		//
		try {
			results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), "pingu", "pingu");
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			assertEquals("ERROR: permission denied for relation testTable", e.getMessage());
		}
		
		//
		// Now drop the DB
		//
		Response deleteResponse = getClient().path("/24/MAIN").delete();
		assertEquals(200, deleteResponse.getStatus());
		
	}
	
	@Test
	public void checkNoAccessFromOtherRoles(){
		
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

}
