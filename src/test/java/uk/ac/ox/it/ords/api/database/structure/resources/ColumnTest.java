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

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;

public class ColumnTest extends AbstractDatabaseTestRunner {
	
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
	public void createColumn(){
		
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
	}
	
	@Test
	public void getColumnUnauth(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		logout();
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(403, response.getStatus());
		
		loginUsingSSO("pinga@nowhere.co","pinga@nowhere.co");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(403, response.getStatus());
		logout();
	}
	
	
	@Test
	public void createColumnUnauth(){
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(403, response.getStatus());
		
		loginUsingSSO("pinga@nowhere.co","pinga@nowhere.co");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(403, response.getStatus());
		logout();
	}
	
	
	@Test
	public void createAutoIncrementColumn(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("auto", "int", null, false, true);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		assertEquals(200, response.getStatus());
		
		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertTrue(column.isAutoincrement());

		logout();
	}
	
	@Test
	public void createAutoIncrementColumnWithWrongType(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("auto", "varchar", null, false, true);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(400, response.getStatus());

		logout();
	}
	
	@Test
	public void createAutoIncrementColumnWithDefault(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("auto", "int", "0", false, true);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(400, response.getStatus());

		logout();
	}
	
	@Test
	public void createColumnWithDefault(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testcol", "varchar", "banana", true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcol/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcol/false").get();
		assertEquals(200, response.getStatus());
		
		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertEquals("'banana'::character varying", column.getDefaultvalue());

		logout();
	}
	
	@Test
	public void createNotNullColumnWithoutDefault(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testcol", "varchar", null, false, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcol/false").post(column1);
		assertEquals(400, response.getStatus());

		logout();
	}
	
	@Test
	public void createColumnWithoutType(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		ColumnRequest column1 = this.buildColumnRequest("testcol", null, "banana", true, false);
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcol/false").post(column1);
		assertEquals(400, response.getStatus());
		logout();
	}
	
	
	@Test
	public void createColumnWithNull(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcol/false").post(null);
		assertEquals(400, response.getStatus());
		logout();
	}
	
	@Test
	public void deleteColumn(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testcol", "varchar", "banana", true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcol/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcol/false").get();
		assertEquals(200, response.getStatus());
		
		// Delete
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcol/false").delete();
		assertEquals(200, response.getStatus());
		
		// Check column no longer exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcol/false").get();
		assertEquals(404, response.getStatus());

		logout();
	}
	
	@Test
	public void getNonexistingColumn(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// Check column exists
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcolx/false").get();
		assertEquals(404, response.getStatus());

		logout();
	}
	
	@Test
	public void deleteNonexistingColumn(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// Check column exists
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testcolx/false").delete();
		assertEquals(404, response.getStatus());

		logout();
	}
	
	@Test
	public void updateColumnToAuto(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("auto", "int", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		assertEquals(200, response.getStatus());
		
		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertFalse(column.isAutoincrement());
		
		// Update
		requestStruct request = new requestStruct();
		request.autoincrement = "true";
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").put(request);
		assertEquals(200, response.getStatus());

		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		column = response.readEntity(ColumnRequest.class);
		assertTrue(column.isAutoincrement());

		logout();
	}
	
	@Test
	public void updateColumnToAutoNonInteger(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("auto", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		assertEquals(200, response.getStatus());
		
		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertFalse(column.isAutoincrement());
		
		// Update
		requestStruct request = new requestStruct();
		request.autoincrement = "true";
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").put(request);
		assertEquals(400, response.getStatus());

		logout();
	}
	
	@Test
	public void updateColumnToAutoWithDefault(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("auto", "int", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		assertEquals(200, response.getStatus());
		
		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertFalse(column.isAutoincrement());
		
		// Update
		requestStruct request = new requestStruct();
		request.autoincrement = "true";
		request.defaultvalue = "0";
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").put(request);
		assertEquals(400, response.getStatus());

		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		column = response.readEntity(ColumnRequest.class);
		assertFalse(column.isAutoincrement());

		logout();
	}
	
	@Test
	public void updateColumnRemoveAuto(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("auto", "int", null, false, true);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		assertEquals(200, response.getStatus());
		
		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertTrue(column.isAutoincrement());
		
		// Update
		requestStruct request = new requestStruct();
		request.autoincrement = "false";
		request.defaultvalue = "";
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").put(request);
		assertEquals(200, response.getStatus());

		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		column = response.readEntity(ColumnRequest.class);
		assertFalse(column.isAutoincrement());

		logout();
	}
	
	@Test
	public void updateColumnNonexisting(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		requestStruct request = new requestStruct();
		request.autoincrement = "false";
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/nope/false").put(request);
		assertEquals(404, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/nope/column/nope/false").put(request);
		assertEquals(404, response.getStatus());

		logout();
	}
	
	@Test
	public void updateColumnToAutoAgain(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("auto", "int", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		assertEquals(200, response.getStatus());
		
		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertFalse(column.isAutoincrement());
		
		// Update
		requestStruct request = new requestStruct();
		request.autoincrement = "true";
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").put(request);
		assertEquals(200, response.getStatus());

		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").get();
		column = response.readEntity(ColumnRequest.class);
		assertTrue(column.isAutoincrement());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").put(request);
		assertEquals(400, response.getStatus());

		logout();
	}
	
	@Test
	public void updateColumnWithNull(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		ColumnRequest column1 = this.buildColumnRequest("auto", "int", null, true, false);
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").post(column1);
		assertEquals(201, response.getStatus());
		
		requestStruct request = new requestStruct();
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/auto/false").put(request);
		assertEquals(400, response.getStatus());

		logout();
	}
	
	@Test
	public void renameColumn(){
				
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("test1", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").get();
		assertEquals(200, response.getStatus());
		
		// Update
		requestStruct request = new requestStruct();
		request.newname = "test2";
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").put(request);
		assertEquals(200, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test2/false").get();
		assertEquals(200, response.getStatus());


		logout();
	}
	
	@Test
	public void updateColumnWithNullAndDefaults(){
				
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("test1", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").get();
		assertEquals(200, response.getStatus());
		
		// Update
		requestStruct request = new requestStruct();
		request.defaultvalue = "banana";
		request.nullable = "false";
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").put(request);
		assertEquals(200, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").get();
		assertEquals(200, response.getStatus());

		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertFalse(column.isNullable());
		assertEquals("'banana'::character varying", column.getDefaultvalue());
		
		logout();
	}
	
	@Test
	public void updateColumnTypeFromVarcharToText(){
				
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("test1", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").get();
		assertEquals(200, response.getStatus());
		
		// Update
		requestStruct request = new requestStruct();
		request.datatype="text";
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").put(request);
		assertEquals(200, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/test1/false").get();
		assertEquals(200, response.getStatus());

		ColumnRequest column = response.readEntity(ColumnRequest.class);
		assertEquals("text", column.getDatatype());

		
		logout();
	}
	
	
	class requestStruct {
		public String newname;
		public String autoincrement;
		public String defaultvalue;
		public String nullable;
		public String datatype;
	}
	

}
