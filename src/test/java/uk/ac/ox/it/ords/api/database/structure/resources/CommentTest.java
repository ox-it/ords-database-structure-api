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

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.CommentRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;

public class CommentTest extends AbstractDatabaseTestRunner{
	
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
	public void addTableComment(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		CommentRequest commentRequest = new CommentRequest();
		commentRequest.setComment("Hello World");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/comment/false").post(commentRequest);
		assertEquals(201, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/comment/false").get();
		assertEquals(200, response.getStatus());
		CommentRequest commentResponse = response.readEntity(CommentRequest.class);
		assertEquals("Hello World", commentResponse.getComment());
		logout();
	}
	
	@Test
	public void getTableCommentNoTable(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/notable/comment/false").get();
		assertEquals(404, response.getStatus());
		logout();
	}
	
	@Test
	public void addTableCommentNoTable(){
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		CommentRequest commentRequest = new CommentRequest();
		commentRequest.setComment("Hello World");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/notable/comment/false").post(commentRequest);
		assertEquals(404, response.getStatus());
		logout();
	}
	
	@Test
	public void addTableCommentNotAuth(){
		CommentRequest commentRequest = new CommentRequest();
		commentRequest.setComment("Hello World");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/comment/false").post(commentRequest);
		assertEquals(403, response.getStatus());
	}
	
	@Test
	public void createColumnComment(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());
		
		// Add comment
		CommentRequest commentRequest = new CommentRequest();
		commentRequest.setComment("Hello World");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/comment/false").post(commentRequest);
		assertEquals(201, response.getStatus());
		
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/comment/false").get();
		assertEquals(200, response.getStatus());
		CommentRequest commentResponse = response.readEntity(CommentRequest.class);
		assertEquals("Hello World", commentResponse.getComment());

		logout();
	}
	
	@Test
	public void getColumnCommentNoColumn(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// Add comment
		CommentRequest commentRequest = new CommentRequest();
		commentRequest.setComment("Hello World");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/comment/false").get();
		assertEquals(404, response.getStatus());

		logout();
	}
	
	@Test
	public void createColumnCommentNoColumn(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// Add comment
		CommentRequest commentRequest = new CommentRequest();
		commentRequest.setComment("Hello World");
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/comment/false").post(commentRequest);
		assertEquals(404, response.getStatus());

		logout();
	}
}