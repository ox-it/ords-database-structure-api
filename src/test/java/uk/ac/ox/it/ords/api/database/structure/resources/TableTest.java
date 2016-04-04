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

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.TableRenameRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;

public class TableTest extends AbstractDatabaseTestRunner{

	int physicalDatabaseId;
	
	@Before
	public void setupDatabase(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
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
		Response response = getClient().path("/"+physicalDatabaseId+"/table/notable/false").delete();
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
	
}
