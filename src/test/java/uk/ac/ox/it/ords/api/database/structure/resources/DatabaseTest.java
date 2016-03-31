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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;

import javax.ws.rs.core.Response;

import org.apache.shiro.SecurityUtils;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.CommentRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.ConstraintRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.IndexRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.PositionRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.TablePosition;
import uk.ac.ox.it.ords.api.database.structure.dto.TableRenameRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsDB;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.HibernateUtils;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.model.UserRole;
import uk.ac.ox.it.ords.security.services.PermissionsService;

public class DatabaseTest extends AbstractDatabaseTest {
	
	enum constraint_type{UNIQUE, PRIMARY, FOREIGN}
	
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
	
	@Test
	public void checkPermission() throws Exception{
		loginUsingSSO("test","test");
		
		Session session = HibernateUtils.getSessionFactory().getCurrentSession();
		Transaction transaction = session.beginTransaction();
		UserRole pingu = new UserRole();
		pingu.setPrincipalName("test");
		pingu.setRole("databaseowner_9");
		session.save(pingu);
		
		Permission permission = new Permission();
		permission.setRole("databaseowner_9");
		permission.setPermission("database:*:9");
		PermissionsService.Factory.getInstance().createPermission(permission);
		transaction.commit();
		
		System.out.println(SecurityUtils.getSubject().hasRole("databaseowner_9"));
		assertTrue(SecurityUtils.getSubject().isPermitted("database:view:9"));
		assertTrue(SecurityUtils.getSubject().isPermitted("database:view:9:43"));
		assertTrue(SecurityUtils.getSubject().isPermitted("database:view:9:41"));

	}
	
	@Test
	public void getDatabaseList() {
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response response = getClient().path("/").get();
		assertEquals(200, response.getStatus());
		logout();
	}

	@Test
	public void createAndDeleteDatabase() {
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/0/MAIN").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		String dbId = Integer.toString(db.getPhysicalDatabaseId());
		AbstractResourceTest.databaseIds.put(dbId, db.getEntityType().toString());
		
		// Strip the id from the end of the path
		int dbID = db.getPhysicalDatabaseId();
		
		response = getClient().path("/"+dbID+"/MAIN").delete();
		assertEquals(200, response.getStatus());
		AbstractResourceTest.databaseIds.remove(dbId);
		
		logout();
	}
	
	
	@Test
	public void stagingTest() {
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/0/MAIN").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		
		// Strip the id from the end of the path
		int dbID = db.getPhysicalDatabaseId();
		
		// create a staging version
		response = getClient().path("/"+dbID+"/MAIN/staging").post(null);
		assertEquals(201, response.getStatus());
		
		// merge it down to actual
		response = getClient().path("/"+dbID+"/MAIN/staging").put(null);
		assertEquals(200, response.getStatus());
		
		// delete the original
		response = getClient().path("/"+dbID+"/MAIN").delete();
		assertEquals(200, response.getStatus());
		
		logout();
	}
	
	@Test
	public void testTables() {
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/0/MAIN").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		int dbID = db.getPhysicalDatabaseId();
		
		
		// do stuff
		
		// Create a table
		response = getClient().path("/"+dbID+"/MAIN/table/dummy/false").post(null);
		assertEquals(201, response.getStatus());
		
		// rename the table
		TableRenameRequest tnr = new TableRenameRequest();
		tnr.setNewname("clever");
		
		response = getClient().path("/"+dbID+"/MAIN/table/dummy/false").put(tnr);
		assertEquals(200, response.getStatus());
		
		// get the renamed table
		response = getClient().path("/"+dbID+"/MAIN/table/clever/false").get();
		assertEquals(200, response.getStatus());
		
		// delete the table
		response = getClient().path("/"+dbID+"/MAIN/table/clever/false").delete();
		assertEquals(200, response.getStatus());
		
		
		// delete the original
		response = getClient().path("/"+dbID+"/MAIN").delete();
		assertEquals(200, response.getStatus());
		
		logout();		
	}
	
	
	@Test
	public void testIntegratedBuildTable() {

		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/0/MAIN").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
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
		
		// build another column as auto inc
		ColumnRequest column2 = this.buildColumnRequest("id", "int", null, false, true);
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/column/id/false").post(column2);
		assertEquals(201, response.getStatus());
		
		// create it as primary key
		ArrayList<String> columnNames = new ArrayList<String>();
		columnNames.add("id");
		ConstraintRequest pkey_contstraint = this.buildConstraintRequest("pkey_testTable", constraint_type.PRIMARY, columnNames, "", "");
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/constraint/pkey_testTable/false").post(pkey_contstraint);
		assertEquals(201, response.getStatus());
		
		
		// try making a foreign key
		// build another table
		response = getClient().path("/"+dbID+"/MAIN/table/linkTable/false").post(null);
		assertEquals(201, response.getStatus());
		
		// and a column to link
		ColumnRequest linkColumn = this.buildColumnRequest("other_id", "int", null, true, false);
		response = getClient().path("/"+dbID+"/MAIN/table/linkTable/column/other_id/false").post(linkColumn);
		assertEquals(201, response.getStatus());
		
		
		// set the position of the tables for schema designer
		ArrayList<TablePosition> tablePositionsArray = new ArrayList<TablePosition>();
		TablePosition linkTablePosition = new TablePosition();
		linkTablePosition.setTablename("linkTable");
		linkTablePosition.setX(120);
		linkTablePosition.setY(120);
		TablePosition testTablePosition = new TablePosition();
		testTablePosition.setTablename("testTable");
		testTablePosition.setX(240);
		testTablePosition.setY(240);
		tablePositionsArray.add(linkTablePosition);
		tablePositionsArray.add(testTablePosition);
		PositionRequest positions = new PositionRequest();
		positions.setPositions(tablePositionsArray);
		
		response = getClient().path("/"+dbID+"/MAIN/positions").put(positions);
		assertEquals(200, response.getStatus());
		
		// create to the relationship as a constraint
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("other_id");
		ConstraintRequest constraint = this.buildConstraintRequest("link_constraint", constraint_type.FOREIGN, columns, "testTable", "id");
		response = getClient().path("/"+dbID+"/MAIN/table/linkTable/constraint/link_constraint/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		// set a table comment
		String comment = "A bi££are cømment";
		CommentRequest commentRequest = this.buildCommentRequest(comment);
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/comment/false").post(commentRequest);
		assertEquals(201, response.getStatus());
		
		// set a column comment
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/column/testColumn/comment/false").post(commentRequest);
		assertEquals(201, response.getStatus());
		
		// rename the PK constraint
		pkey_contstraint = this.buildConstraintRequest("pkey_testTableRenamed", constraint_type.PRIMARY, columnNames, "", "");
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/constraint/pkey_testTable_1/false").put(pkey_contstraint);
		assertEquals(200, response.getStatus());
		
		// delete the FK constraint
		response = getClient().path("/"+dbID+"/MAIN/table/linkTable/constraint/link_constraint_2/false").delete();
		assertEquals(200, response.getStatus());
		
		// delete the PK constraint
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/constraint/pkey_testTableRenamed/false").delete();
		assertEquals(200, response.getStatus());
		
		// create an index
		columns.clear();
		columns.add("testColumn");
		IndexRequest index = this.buildIndexRequest("testIndex", false, columns);
		response = getClient().path("/"+dbID+"/MAIN/table/testTable/index/testIndex/false").post(index);
		assertEquals(201, response.getStatus());
		
		
		
		// get the whole database structure
		response = getClient().path("/"+dbID+"/MAIN").get();
		assertEquals(200, response.getStatus());
		TableList tableList = (TableList)response.readEntity(TableList.class);
		@SuppressWarnings("rawtypes")
		HashMap tables = tableList.getTables();
		assertEquals(2, tables.size());
		
		
		// delete the original
		response = getClient().path("/"+dbID+"/MAIN").delete();
		assertEquals(200, response.getStatus());
		
		logout();
	}
	
}
