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

import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;


public class StagingTest extends AbstractDatabaseTestRunner {
	
	@Test
	public void createStagingDatabase(){
		
		// Create a database
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "test");
		Response response = getClient().path("/").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		// Strip the id from the end of the path
		int physicalDatabaseId = db.getPhysicalDatabaseId();
		
		// Now make a staging version
		response = getClient().path("/"+physicalDatabaseId+"/staging").post(dbr);
		assertEquals(201, response.getStatus());
		assertTrue(response.getLocation().getPath().endsWith("/staging"));

		// Check it exists
		response = getClient().path("/"+physicalDatabaseId+"/staging").get();
		assertEquals(200, response.getStatus());
		
		// Drop staging - unauth
		logout();
		assertEquals(403, getClient().path("/"+physicalDatabaseId+"/staging").delete().getStatus());
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// Drop staging
		response = getClient().path("/"+physicalDatabaseId+"/staging").delete();
		assertEquals(200, response.getStatus());

		// Check it was dropped
		response = getClient().path("/"+physicalDatabaseId+"/staging").get();
		assertEquals(404, response.getStatus());

		// Drop main
		response = getClient().path("/"+physicalDatabaseId+"/").delete();
		assertEquals(200, response.getStatus());
		AbstractResourceTest.databaseIds.remove(logicalDatabaseId);
		
	}
	
	@Test
	public void createStagingNoexistingVersion(){
		
		// Create a database
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "test");
		Response response = getClient().path("/").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		// Strip the id from the end of the path
		int physicalDatabaseId = db.getPhysicalDatabaseId();

		// Drop staging
		response = getClient().path("/"+physicalDatabaseId+"/staging").delete();
		assertEquals(404, response.getStatus());

		// Drop main
		response = getClient().path("/"+physicalDatabaseId+"/").delete();
		assertEquals(200, response.getStatus());
		logout();
		
	}
	
	@Test
	public void dropStagingNonexistant(){
		Response response = getClient().path("/999999/staging").delete();
		assertEquals(404, response.getStatus());
	}
	@Test
	public void getStagingNonexistant(){
		Response response = getClient().path("/999999/staging").get();
		assertEquals(404, response.getStatus());
	}
	@Test
	public void createStagingNonexistant(){
		Response response = getClient().path("/999999/staging").post(null);
		assertEquals(404, response.getStatus());
	}
	
	@Test
	public void mergeStaging(){
		// Create a database
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "test");
		Response response = getClient().path("/").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		// Strip the id from the end of the path
		int physicalDatabaseId = db.getPhysicalDatabaseId();

		// Now make a staging version
		response = getClient().path("/"+physicalDatabaseId+"/staging").post(dbr);
		assertEquals(201, response.getStatus());
		
		// Get its ID
		String path = response.getLocation().getPath();
		String stagingId = path.split("/")[1];

		// Check it exists
		response = getClient().path("/"+stagingId+"/staging").get();
		assertEquals(200, response.getStatus());
		
		// Merge staging into main
		response = getClient().path("/"+stagingId+"/staging").put(null);
		assertEquals(200, response.getStatus());

		// Drop main
		response = getClient().path("/"+physicalDatabaseId+"/").delete();
		assertEquals(200, response.getStatus());
	}


}
