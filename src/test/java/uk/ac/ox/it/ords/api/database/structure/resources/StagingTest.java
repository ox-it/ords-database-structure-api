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

import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;


public class StagingTest extends AbstractDatabaseTestRunner {
	
	@Test
	public void createStagingDatabase(){
		
		// Create a database
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/0/MAIN").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		// Strip the id from the end of the path
		int physicalDatabaseId = db.getPhysicalDatabaseId();
		
		// Now make a staging version
		response = getClient().path("/"+physicalDatabaseId+"/MAIN/staging").post(dbr);
		assertEquals(201, response.getStatus());

		// Check it exists
		response = getClient().path("/"+physicalDatabaseId+"/MAIN/staging").get();
		assertEquals(200, response.getStatus());
		
		// Drop staging
		response = getClient().path("/"+physicalDatabaseId+"/MAIN/staging").delete();
		assertEquals(200, response.getStatus());

		// Check it was dropped
		response = getClient().path("/"+physicalDatabaseId+"/MAIN/staging").get();
		assertEquals(404, response.getStatus());

		// Drop main
		response = getClient().path("/"+physicalDatabaseId+"/MAIN").delete();
		assertEquals(200, response.getStatus());
		AbstractResourceTest.databaseIds.remove(logicalDatabaseId);
		
	}
	
	@Test
	public void dropStagingNonexistant(){
		Response response = getClient().path("/999999/MAIN/staging").delete();
		assertEquals(404, response.getStatus());
	}
	@Test
	public void getStagingNonexistant(){
		Response response = getClient().path("/999999/MAIN/staging").get();
		assertEquals(404, response.getStatus());
	}
	@Test
	public void createStagingNonexistant(){
		Response response = getClient().path("/999999/MAIN/staging").post(null);
		assertEquals(404, response.getStatus());
	}
	
	@Test
	public void mergeStaging(){
		// Create a database
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/0/MAIN").post(dbr);
		assertEquals(201, response.getStatus());
		
		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		// Strip the id from the end of the path
		int physicalDatabaseId = db.getPhysicalDatabaseId();
		
		// Now make a staging version
		response = getClient().path("/"+physicalDatabaseId+"/MAIN/staging").post(dbr);
		assertEquals(201, response.getStatus());

		// Check it exists
		response = getClient().path("/"+physicalDatabaseId+"/MAIN/staging").get();
		assertEquals(200, response.getStatus());
		
		// Merge staging
		response = getClient().path("/"+physicalDatabaseId+"/MAIN/staging").put(null);
		assertEquals(200, response.getStatus());

		// Drop main
		response = getClient().path("/"+physicalDatabaseId+"/MAIN").delete();
		assertEquals(200, response.getStatus());
		AbstractResourceTest.databaseIds.remove(logicalDatabaseId);
	}


}