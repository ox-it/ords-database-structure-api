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

import java.util.ArrayList;

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.IndexRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;

public class IndexTest extends AbstractDatabaseTestRunner{
	
	int physicalDatabaseId;

	@Before
	public void setupTable(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		// Strip the id from the end of the path
		physicalDatabaseId = db.getPhysicalDatabaseId();
		
		// Create a table
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").post(null);
		assertEquals(201, response.getStatus());
		logout();
	}
	
	@After
	public void tearDownTable(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/false").delete();
		assertEquals(200, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId).delete();
		assertEquals(200, response.getStatus());
		AbstractResourceTest.databaseIds.remove(logicalDatabaseId);
		
		logout();

	}

	
	@Test
	public void getIndexNonexisting(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").get();
		assertEquals(404, response.getStatus());
		logout();
	}
	
	@Test
	public void getIndexNonexisting2(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response response = getClient().path("/99/table/testtable/index/testindex/false").get();
		assertEquals(404, response.getStatus());
		logout();
	}
	
	@Test
	public void getIndexUnauth(){
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").get();
		assertEquals(403, response.getStatus());
	}
	
	@Test
	public void addIndex(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());
		
		// Add index
		IndexRequest request = new IndexRequest();
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("testColumn");
		request.setColumns(columns);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").post(request);
		assertEquals(201, response.getStatus());
		
		// Get index
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").get();
		assertEquals(200, response.getStatus());

		logout();
	}
	
	@Test
	public void addIndexUnauth(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());
		
		logout();
		
		// Add index
		IndexRequest request = new IndexRequest();
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("testColumn");
		request.setColumns(columns);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").post(request);
		assertEquals(403, response.getStatus());
	}

	@Test
	public void addIndexNoColumns(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());
		
		// Add index
		IndexRequest request = new IndexRequest();
		request.setColumns(null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").post(request);
		assertEquals(400, response.getStatus());

		logout();
	}

	@Test
	public void renameIndex(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());
		
		// Add index
		IndexRequest request = new IndexRequest();
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("testColumn");
		request.setColumns(columns);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").post(request);
		assertEquals(201, response.getStatus());
		
		// Update index
		request.setNewname("newidx");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").put(request);
		assertEquals(200, response.getStatus());
		
		// Get index
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/newidx/false").get();
		assertEquals(200, response.getStatus());

		logout();
	}
	
	@Test
	public void renameIndexNull(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());
		
		// Add index
		IndexRequest request = new IndexRequest();
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("testColumn");
		request.setColumns(columns);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").post(request);
		assertEquals(201, response.getStatus());
		
		// Update index
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").put(request);
		assertEquals(400, response.getStatus());

		logout();
	}
	
	@Test
	public void renameIndexNonexistant(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		IndexRequest request = new IndexRequest();
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("testColumn");
		request.setColumns(columns);
		
		// Update index
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").put(request);
		assertEquals(404, response.getStatus());

		logout();
	}
	
	@Test
	public void deleteIndex(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());
		
		// Add index
		IndexRequest request = new IndexRequest();
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("testColumn");
		request.setColumns(columns);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").post(request);
		assertEquals(201, response.getStatus());
		
		// Delete index
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").delete();
		assertEquals(200, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").get();
		assertEquals(404, response.getStatus());

		logout();
	}
	
	@Test
	public void deleteIndexUnauth(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());
		
		// Add index
		IndexRequest request = new IndexRequest();
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("testColumn");
		request.setColumns(columns);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").post(request);
		assertEquals(201, response.getStatus());
		
		logout();
		
		// Delete index
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").delete();
		assertEquals(403, response.getStatus());
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/index/testindex/false").get();
		assertEquals(200, response.getStatus());

		logout();
	}

}
