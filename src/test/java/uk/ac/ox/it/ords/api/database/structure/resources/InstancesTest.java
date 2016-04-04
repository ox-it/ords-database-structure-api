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

import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;

public class InstancesTest extends AbstractDatabaseTestRunner{

	@Test
	public void createTestVersion() throws Exception{

		// Create a database
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/").post(dbr);
		assertEquals(201, response.getStatus());

		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		int physicalDatabaseId = db.getPhysicalDatabaseId();
		
		// Now make a TEST version
		dbr.setInstance("TEST");
		response = getClient().path("/"+physicalDatabaseId+"/").post(dbr);
		assertEquals(201, response.getStatus());
		db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		
		// Check the TEST version exists
		response = getClient().path("/"+db.getPhysicalDatabaseId()+"/").get();
		assertEquals(200, response.getStatus());
		boolean exists = DatabaseStructureService.Factory.getInstance().checkDatabaseExists(db.getPhysicalDatabaseId(), false);
		assertTrue(exists);
		
		// Drop test
		response = getClient().path("/"+db.getPhysicalDatabaseId()+"/").delete();
		assertEquals(200, response.getStatus());
		
		// Drop main
		response = getClient().path("/"+physicalDatabaseId+"/").delete();
		assertEquals(200, response.getStatus());
	}

	@Test
	public void mergeTestIntoMain() throws Exception{

		// Create a database
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		DatabaseRequest dbr = this.buildDatabaseRequest(null, logicalDatabaseId, "localhost");
		Response response = getClient().path("/").post(dbr);
		assertEquals(201, response.getStatus());

		OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(db);
		
		int physicalDatabaseId = db.getPhysicalDatabaseId();
		
		// Now make a TEST version
		dbr.setInstance("TEST");
		response = getClient().path("/"+physicalDatabaseId+"/").post(dbr);
		assertEquals(201, response.getStatus());
		db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		
		// Check the TEST version exists
		response = getClient().path("/"+db.getPhysicalDatabaseId()+"/").get();
		assertEquals(200, response.getStatus());
		boolean exists = DatabaseStructureService.Factory.getInstance().checkDatabaseExists(db.getPhysicalDatabaseId(), false);
		assertTrue(exists);
		
		// Merge test into main
		dbr.setCloneFrom(db.getPhysicalDatabaseId());
		response = getClient().path("/"+physicalDatabaseId+"/").put(dbr);
		assertEquals(200, response.getStatus());
		
		//
		// We should now have no test instance
		//
		response = getClient().path("/"+db.getPhysicalDatabaseId()+"/").get();
		assertEquals(404, response.getStatus());
		
		// Drop main
		response = getClient().path("/"+physicalDatabaseId+"/").delete();
		assertEquals(200, response.getStatus());
	}
}
