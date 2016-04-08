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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import javax.ws.rs.core.Response;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.OdbcResponse;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissionSets;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissions;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.StructureODBCService;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.HibernateUtils;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.model.UserRole;
import uk.ac.ox.it.ords.security.services.PermissionsService;

/**
 * Tests for ODBC services
 * Make sure you've set up PostgreSQL to require passwords in pg_hba.conf, e.g.:
 * host    all             all             127.0.0.1/32            md5
 * @author scottbw
 *
 */
public class OdbcTest extends AbstractDatabaseTest {
	
	private static int projectId = 9999;
	private int dbID;
	private static int logicalDatabaseId;

	private OrdsPhysicalDatabase db;
	
	@BeforeClass
	public static void setupContext() throws Exception{
		
		logicalDatabaseId = 101;
		
		//
		// Enable ODBC
		//
		Permission permission = new Permission();
		permission.setPermission(DatabaseStructurePermissions.DATABASE_REQUEST_ODBC_ACCESS(logicalDatabaseId));
		permission.setRole("owner_"+projectId);
		PermissionsService.Factory.getInstance().createPermission(permission);
		permission = new Permission();
		permission.setPermission(DatabaseStructurePermissions.DATABASE_REQUEST_ODBC_ACCESS(logicalDatabaseId));
		permission.setRole("viewer_"+projectId);
		PermissionsService.Factory.getInstance().createPermission(permission);
		permission = new Permission();		
		permission.setPermission(DatabaseStructurePermissions.DATABASE_REQUEST_ODBC_ACCESS(logicalDatabaseId));
		permission.setRole("contributor_"+projectId);
		PermissionsService.Factory.getInstance().createPermission(permission);
		
	}
	
	@Before
	public void setup() throws Exception{
		
		//
		// Create a database
		//
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/").post(dbr);
		assertEquals(201, response.getStatus());
		db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		//
		// Add a table
		//
		dbID = db.getPhysicalDatabaseId();
		
		// Create a table
		response = getClient().path("/"+dbID+"/table/testTable/false").post(null);
		assertEquals(201, response.getStatus());
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		// Create a column
		response = getClient().path("/"+dbID+"/table/testTable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		logout();
	}
	
	@After
	public void tearDown() throws Exception{

		//
		// Now drop the DB
		//
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		OrdsPhysicalDatabase database = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbID);
		String server = database.getDatabaseServer();
		String databaseName = database.getDbConsumedName();

		Response deleteResponse = getClient().path("/"+dbID+"/").delete();
		assertEquals(200, deleteResponse.getStatus());

		//
		// Check there are no special ODBC roles for the deleted database
		//
		assertTrue(StructureODBCService.Factory.getInstance().getAllODBCRolesForDatabase(server, databaseName).isEmpty());
		
		logout();
	}

	
	@Test 
	public void createOdbcRoleUnauth() throws Exception{
		Response odbcResponse = getClient().path("/99/odbc/").post(null);
		assertEquals(401, odbcResponse.getStatus());
	}
	
	@Test 
	public void createOdbcRoleNoDb() throws Exception{
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response odbcResponse = getClient().path("/99/odbc/").post(null);
		assertEquals(404, odbcResponse.getStatus());
		logout();
	}
	
	@Test 
	public void revokeOdbcRoleUnauth() throws Exception{
		Response odbcResponse = getClient().path("/99/odbc/pingu").delete();
		assertEquals(401, odbcResponse.getStatus());
	}
	
	@Test 
	public void revokeOdbcRoleNoDb() throws Exception{
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response odbcResponse = getClient().path("/99/odbc/pingu").delete();
		assertEquals(404, odbcResponse.getStatus());
		logout();
	}

	@Test
	public void createOdbcRole() throws Exception{

		//
		// Add Pinga as a user for this database
		//
		this.addViewer("pinga@penguins.com", logicalDatabaseId);
		
		//
		// Now lets add an ODBC role - this time Pinga is logging in and asking for access
		//
		loginUsingSSO("pinga@penguins.com","");

		Response odbcResponse = getClient().path("/"+dbID+"/odbc/").post(null);
		assertEquals(200, odbcResponse.getStatus());
		OdbcResponse output = odbcResponse.readEntity(OdbcResponse.class);
		String password = output.getPassword();
		String username = output.getUsername();
		
		//
		// So, do we now have rights for pinga? Easiest way is to see if we can connect and run a query. It won't
		// return anything as its an empty table, but we'll get an Exception if the role isn't permitted.
		//
		CachedRowSet results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
		assertFalse(results.first());
		
		//
		// This should fail as pinga should only have viewer access
		//
		try {
			results = runSQLStatement("insert into \"testTable\" values ('testColumn' = 'banana')", "localhost", calculateInstanceName(db, "MAIN"), username, password);
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
		odbcResponse = getClient().path("/"+dbID+"/odbc/"+username).delete();
		assertEquals(200, odbcResponse.getStatus());
		
		//
		// This should now fail as pinga is no longer allowed access
		//
		try {
			results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			// We should get a message about authentication failing at this point
		}
		logout();
	}
	
	@Test
	public void revokeOdbcRole() throws Exception{
		
		//
		// Add Pinga as a user for this database
		//
		this.addViewer("pinga@penguins.com", dbID);
		
		//
		// Now lets add an ODBC role - this time Pinga is logging in and asking for access
		//
		loginUsingSSO("pinga@penguins.com","");

		Response odbcResponse = getClient().path("/"+dbID+"/odbc/").post(null);
		assertEquals(200, odbcResponse.getStatus());
		OdbcResponse output = odbcResponse.readEntity(OdbcResponse.class);
		String password = output.getPassword();
		String username = output.getUsername();
		
		//
		// So, do we now have rights for pinga? Easiest way is to see if we can connect and run a query. It won't
		// return anything as its an empty table, but we'll get an Exception if the role isn't permitted.
		//
		CachedRowSet results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
		assertFalse(results.first());
		
		//
		// This should fail as pinga should only have viewer access
		//
		try {
			results = runSQLStatement("insert into \"testTable\" values ('testColumn' = 'banana')", "localhost", calculateInstanceName(db, "MAIN"), username, password);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			assertEquals("ERROR: permission denied for relation testTable", e.getMessage());
		}
		
		//
		// Now drop pinga - this should fail as pinga doesn't have the rights
		//
		odbcResponse = getClient().path("/"+dbID+"/odbc/"+username).delete();
		assertEquals(403, odbcResponse.getStatus());
		
		logout();
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		odbcResponse = getClient().path("/"+dbID+"/odbc/"+username).delete();
		assertEquals(200, odbcResponse.getStatus());
		
		//
		// This should now fail as pinga is no longer allowed access
		//
		try {
			results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			// We should get a message about authentication failing at this point
		}
		logout();
		
	}
	
	@Test
	public void revokeAllOdbcRoles() throws Exception{
		
		//
		// Add Pinga as a user for this database
		//
		this.addViewer("pinga@penguins.com", dbID);
		
		//
		// Now lets add an ODBC role - this time Pinga is logging in and asking for access
		//
		loginUsingSSO("pinga@penguins.com","");

		Response odbcResponse = getClient().path("/"+dbID+"/odbc/").post(null);
		assertEquals(200, odbcResponse.getStatus());
		OdbcResponse output = odbcResponse.readEntity(OdbcResponse.class);
		String password = output.getPassword();
		String username = output.getUsername();
		
		//
		// So, do we now have rights for pinga? Easiest way is to see if we can connect and run a query. It won't
		// return anything as its an empty table, but we'll get an Exception if the role isn't permitted.
		//
		CachedRowSet results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
		assertFalse(results.first());
		
		//
		// This should fail as pinga should only have viewer access
		//
		try {
			results = runSQLStatement("insert into \"testTable\" values ('testColumn' = 'banana')", "localhost", calculateInstanceName(db, "MAIN"), username, password);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			assertEquals("ERROR: permission denied for relation testTable", e.getMessage());
		}
		
		//
		// This should also fail
		//
		odbcResponse = getClient().path("/"+dbID+"/odbc/").delete();
		assertEquals(403, odbcResponse.getStatus());
		
		logout();
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		odbcResponse = getClient().path("/"+dbID+"/odbc/").delete();
		assertEquals(200, odbcResponse.getStatus());
		
		//
		// This should now fail as pinga is no longer allowed access
		//
		try {
			results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			// We should get a message about authentication failing at this point
		}
		logout();
		
	}
	
	@Test
	public void createOdbcRoleNotPermitted() throws Exception{
		
		//
		// Create a second logical database
		//
		int logicalDatabaseId2 = 66;
		
		//
		// Create a second database
		//
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId2, "localhost");
		Response response = getClient().path("/").post(dbr);
		assertEquals(201, response.getStatus());
		OrdsPhysicalDatabase db2 = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db2);
		
		//
		// Add a table
		//
		int dbID2 = db2.getPhysicalDatabaseId();
		response = getClient().path("/"+dbID2+"/table/testTable/false").post(null);
		assertEquals(201, response.getStatus());
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		response = getClient().path("/"+dbID2+"/table/testTable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		logout();
		
		//
		// Add Pinga as a user for the first database
		//
		this.addViewer("pinga@penguins.com", logicalDatabaseId);
		
		//
		// Now lets add an ODBC role - this time Pinga is logging in and asking for access
		//
		loginUsingSSO("pinga@penguins.com","");

		Response odbcResponse = getClient().path("/"+dbID+"/odbc/").post(null);
		assertEquals(200, odbcResponse.getStatus());
		OdbcResponse output = odbcResponse.readEntity(OdbcResponse.class);
		String password = output.getPassword();
		String username = output.getUsername();
		
		//
		// So, do we now have rights for pinga? Easiest way is to see if we can connect and run a query. It won't
		// return anything as its an empty table, but we'll get an Exception if the role isn't permitted.
		//
		CachedRowSet results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
		assertFalse(results.first());
		
		//
		// However, we should not have access to the second db
		//
		try {
			results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db2, "MAIN"), username, password);
			fail();
		} catch (Exception e) {
			assertEquals("ERROR: permission denied for relation testTable", e.getMessage());
		}
		
		//
		// As we don't have an ORDS role for Pinga in Db2, we shouldn't be able to request an ODBC role
		//
		odbcResponse = getClient().path("/"+dbID2+"/odbc/").post(null);
		assertEquals(403, odbcResponse.getStatus());
		
		//
		// Now drop the DBs
		//
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response deleteResponse = getClient().path("/"+dbID2+"").delete();
		assertEquals(200, deleteResponse.getStatus());
		logout();
	}
	
	@Test
	public void createOdbcContributorRole() throws Exception{
		
		//
		// Add Pinga as a user for this database
		//
		this.addContributor("pinga@penguins.com", logicalDatabaseId);
		
		//
		// Now lets add an ODBC role - this time Pinga is logging in and asking for access
		//
		loginUsingSSO("pinga@penguins.com","");

		Response odbcResponse = getClient().path("/"+dbID+"/odbc/").post(null);
		assertEquals(200, odbcResponse.getStatus());
		OdbcResponse output = odbcResponse.readEntity(OdbcResponse.class);
		String password = output.getPassword();
		String username = output.getUsername();
		
		//
		// So, do we now have rights for pinga? Easiest way is to see if we can connect and run a query. It won't
		// return anything as its an empty table, but we'll get an Exception if the role isn't permitted.
		//
		CachedRowSet results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
		assertFalse(results.first());
		
		//
		// This should work as pinga has contributor access
		//
		runSQLStatement("insert into \"testTable\" values ('testColumn' = 'banana')", "localhost", calculateInstanceName(db, "MAIN"), username, password);
		results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
		assertTrue(results.first());
		logout();
		
		//
		// Now drop pinga
		//
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		odbcResponse = getClient().path("/"+dbID+"/odbc/"+username).delete();
		assertEquals(200, odbcResponse.getStatus());
		
		//
		// This should now fail as pinga is no longer allowed access
		//
		try {
			results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			// We should get a message about authentication failing at this point
		}
		
		//
		// We also need to drop Pinga's contributor access level or this will mess up other tests
		//
		dropRoles("pinga@penguins.com");
		
		
		logout();
		
	}
	/**
	 * Test that we can reset a user's ODBC password by POSTing
	 * @throws Exception
	 */
	@Test
	public void resetRole() throws Exception{
		
		//
		// Add Pinga as a user for this database
		//
		this.addViewer("pinga@penguins.com", logicalDatabaseId);
		
		//
		// Now lets add an ODBC role - this time Pinga is logging in and asking for access
		//
		loginUsingSSO("pinga@penguins.com","");

		Response odbcResponse = getClient().path("/"+dbID+"/odbc/").post(null);
		assertEquals(200, odbcResponse.getStatus());
		OdbcResponse output = odbcResponse.readEntity(OdbcResponse.class);
		String password = output.getPassword();
		String username = output.getUsername();
		
		odbcResponse = getClient().path("/"+dbID+"/odbc/").post(null);
		assertEquals(200, odbcResponse.getStatus());
		output = odbcResponse.readEntity(OdbcResponse.class);
		String newPassword = output.getPassword();
		assertFalse(password.equals(newPassword));
		
		//
		// This should now fail as we've reset the password
		//
		try {
			CachedRowSet results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, password);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			assertEquals("FATAL: password authentication failed for user \""+username+"\"", e.getMessage());
		}
		
		//
		// So, do we now have rights for pinga? Easiest way is to see if we can connect and run a query. It won't
		// return anything as its an empty table, but we'll get an Exception if the role isn't permitted.
		//
		CachedRowSet results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, newPassword);
		assertFalse(results.first());
		
		//
		// This should fail as pinga should only have viewer access
		//
		try {
			runSQLStatement("insert into \"testTable\" values ('testColumn' = 'banana')", "localhost", calculateInstanceName(db, "MAIN"), username, newPassword);
			fail();
		} catch (Exception e) {
			assertEquals("ERROR: permission denied for relation testTable", e.getMessage());
		}
		
		//
		// Now drop pinga
		//
		logout();
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		odbcResponse = getClient().path("/"+dbID+"/odbc/"+username).delete();
		assertEquals(200, odbcResponse.getStatus());
		
		//
		// This should now fail as pinga is no longer allowed access
		//
		try {
			results = runSQLStatement("SELECT * FROM \"testTable\"", "localhost", calculateInstanceName(db, "MAIN"), username, newPassword);
			assertFalse(results.first());
			fail();
		} catch (Exception e) {
			// We should get a message about authentication failing at this point
		}
		
		logout();
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

		for (String permission : DatabaseStructurePermissionSets.getPermissionsForViewer(databaseid)){
			Permission permissionObject = new Permission();
			permissionObject.setRole("viewer_"+projectId);
			permissionObject.setPermission(permission);
			session.save(permissionObject);
		}
		
		UserRole pinga = new UserRole();
		pinga.setPrincipalName(principal);
		pinga.setRole("viewer_"+projectId);
		session.save(pinga);
		
		transaction.commit();
	}
	
	private void addContributor(String principal, int databaseid){

		Session session = HibernateUtils.getSessionFactory().getCurrentSession();
		Transaction transaction = session.beginTransaction();

		for (String permission : DatabaseStructurePermissionSets.getPermissionsForContributor(databaseid)){
			Permission permissionObject = new Permission();
			permissionObject.setRole("contributor_"+projectId);
			permissionObject.setPermission(permission);
			session.save(permissionObject);
		}
		
		UserRole pinga = new UserRole();
		pinga.setPrincipalName(principal);
		pinga.setRole("contributor_"+projectId);
		session.save(pinga);
		
		transaction.commit();
	}
	
	private void dropRoles(String principal){

		Session session = HibernateUtils.getSessionFactory().getCurrentSession();
		Transaction transaction = session.beginTransaction();

		List<UserRole> roles = session.createCriteria(UserRole.class).add(Restrictions.eq("principalName", principal)).list();
		for (UserRole role: roles){
			session.delete(role);
		}
		
		transaction.commit();
	}
}
