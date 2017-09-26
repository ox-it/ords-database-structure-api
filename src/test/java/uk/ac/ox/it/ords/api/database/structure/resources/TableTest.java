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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.ConstraintRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.TableRenameRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;

public class TableTest extends AbstractDatabaseTestRunner{

	int physicalDatabaseId;
	
	@Before
	public void setupDatabase(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "test");
		Response response = getClient().path("/").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		// Strip the id from the end of the path
		physicalDatabaseId = db.getPhysicalDatabaseId();

		logout();
	}
	
	@After
	public void tearDownDatabase(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");		
		Response response = getClient().path("/"+physicalDatabaseId+"/").delete();
		assertEquals(200, response.getStatus());
		AbstractResourceTest.databaseIds.remove(logicalDatabaseId);
		logout();
	}
	
	@Test
	public void createTable(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");		
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").post(null);
		assertEquals(201, response.getStatus());

		//
		// Try deleting it while logged out
		// 
		logout();
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").delete();
		assertEquals(403, response.getStatus());		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");		

		//
		// Now delete it
		// 
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();
	}
	
	@Test
	public void createTableAlreadyExists(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");		
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").post(null);
		assertEquals(201, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").post(null);
		assertEquals(409, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").delete();
		assertEquals(200, response.getStatus());
	
		
		logout();
	}
	
	@Test
	public void createTableUnauth(){
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").post(null);
		assertEquals(403, response.getStatus());
	}
	
	@Test
	public void getNonexistantTable(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");		
		Response response = getClient().path("/"+physicalDatabaseId+"/table/notable/false").get();
		assertEquals(404, response.getStatus());
		logout();
	}
	
	@Test
	public void deleteNonexistantTable(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");		
		
		Response response = getClient().path("/99999/table/notable/false").delete();
		assertEquals(404, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/notable/false").delete();
		assertEquals(404, response.getStatus());
		logout();
	}
	
	
	@Test
	public void renameTable(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");		
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").post(null);
		assertEquals(201, response.getStatus());
		
		TableRenameRequest request = new TableRenameRequest();
		request.setNewname("testtable2");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").put(request);
		assertEquals(200, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable2/false").get();
		assertEquals(200, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable2/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();
	}
	
	@Test
	public void renameTableNonexisting(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");		
		
		TableRenameRequest request = new TableRenameRequest();
		request.setNewname("testtable2");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").put(request);
		assertEquals(404, response.getStatus());
		
		logout();
	}
	
	@Test
	public void renameTableAndCheckSequences(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").post(null);
		assertEquals(201, response.getStatus());
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("auto", "int", null, false, true);
		
		// Create a column
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(201, response.getStatus());
		
		TableRenameRequest request = new TableRenameRequest();
		request.setNewname("testtable2");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").put(request);
		assertEquals(200, response.getStatus());
		
		// Check renamed table exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable2/false").get();
		assertEquals(200, response.getStatus());
		
		// Check previously named table doesn't
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").get();
		assertEquals(404, response.getStatus());
		
		// Check column exists in renamed table and is still autoinc
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable2/column/auto/false").get();
		assertEquals(200, response.getStatus());
		
		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertTrue(column.isAutoincrement());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable2/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();
	}
	
	@Test
	public void renameTableOverwritingAnother(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");		
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").post(null);
		assertEquals(201, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable2/false").post(null);
		assertEquals(201, response.getStatus());
		
		TableRenameRequest request = new TableRenameRequest();
		request.setNewname("testtable2");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").put(request);
		assertEquals(409, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").get();
		assertEquals(200, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable2/false").get();
		assertEquals(200, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").delete();
		assertEquals(200, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable2/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();
	}
	
	@Test
	public void linkedTablesTest(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");	
		
		// Table 1
		Response response = getClient().path("/"+physicalDatabaseId+"/table/t1/false").post(null);
		assertEquals(201, response.getStatus());
		
		ColumnRequest column1 = this.buildColumnRequest("pk", "int", null, false, true);
		response = getClient().path("/"+physicalDatabaseId+"/table/t1/column/pk/false").post(column1);
		assertEquals(201, response.getStatus());
		
		String[] columns = {"pk"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.PRIMARY, columns, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/t1/constraint/pk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		// Table 2
		response = getClient().path("/"+physicalDatabaseId+"/table/t2/false").post(null);
		assertEquals(201, response.getStatus());
		
		ColumnRequest column2 = this.buildColumnRequest("fk", "int", null, true, false);
		response = getClient().path("/"+physicalDatabaseId+"/table/t2/column/fk/false").post(column2);
		assertEquals(201, response.getStatus());
		
		String[] columns2 = {"fk"};
		constraint = this.buildConstraintRequest("fk", DatabaseTest.constraint_type.FOREIGN, columns2, "t1", "pk");
		response = getClient().path("/"+physicalDatabaseId+"/table/t2/constraint/fk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/t1/false").get();
		assertEquals(200, response.getStatus());
	
		TableList tables = response.readEntity(TableList.class);
		assertEquals(1, tables.getTables().size());
		Map<String,Map> t1 = (Map<String, Map>) tables.getTables().get("t1");
		Map<String,String> t1Columns = (Map<String, String>) t1.get("columns");
		assertEquals(1, t1Columns.size());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/t2/false").get();
		assertEquals(200, response.getStatus());
	
		tables = response.readEntity(TableList.class);
		assertEquals(1, tables.getTables().size());
		Map<String,Map> t2 = (Map<String, Map>) tables.getTables().get("t2");
		Map<String,String> t2Columns = (Map<String, String>) t2.get("columns");
		assertEquals(1, t2Columns.size());
		Map<String,String> relations = (Map<String, String>) t2.get("relations");
		assertEquals(1, relations.size());
		
		logout();
	}
	
}
