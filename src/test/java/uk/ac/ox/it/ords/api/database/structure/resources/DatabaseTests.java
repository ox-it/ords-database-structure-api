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

import java.util.HashMap;

import javax.ws.rs.core.Response;

import org.junit.Test;

import uk.ac.ox.it.ords.api.database.structure.metadata.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;

public class DatabaseTests extends AbstractResourceTest {

	@Test
	public void createAndDeleteDatabase() {
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response response = getClient().path("/database/12/MAIN").post(null);
		assertEquals(201, response.getStatus());
		
		String createPath = response.getLocation().getPath();
		//OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(createPath);
		
		// Strip the id from the end of the path
		String dbID = createPath.substring(createPath.lastIndexOf('/')+1);
		
		response = getClient().path("/database/"+dbID+"/MAIN").delete();
		assertEquals(200, response.getStatus());
		
		logout();
	}
	
	
	@Test
	public void stagingTest() {
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		Response response = getClient().path("/database/1201/MAIN").post(null);
		assertEquals(201, response.getStatus());
		
		String createPath = response.getLocation().getPath();
		//OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
		assertNotNull(createPath);
		
		// Strip the id from the end of the path
		String dbID = createPath.substring(createPath.lastIndexOf('/')+1);
		
		// create a staging version
		response = getClient().path("/database/"+dbID+"/MAIN/staging").post(null);
		assertEquals(200, response.getStatus());
		
		// merge it down to actual
		response = getClient().path("/database/"+dbID+"/MAIN/staging").put(null);
		assertEquals(200, response.getStatus());
		
		// delete the original
		response = getClient().path("/database/"+dbID+"/MAIN").delete();
		assertEquals(200, response.getStatus());
		
		logout();
	}
	
	
	@Test
	public void testBuildTable() {

			loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
			Response response = getClient().path("/database/1205/MAIN").post(null);
			assertEquals(201, response.getStatus());
			
			String createPath = response.getLocation().getPath();
			//OrdsPhysicalDatabase db = (OrdsPhysicalDatabase)response.readEntity(OrdsPhysicalDatabase.class);
			assertNotNull(createPath);
			
			// Strip the id from the end of the path
			String dbID = createPath.substring(createPath.lastIndexOf('/')+1);
			
			// Create a table
			response = getClient().path("/table/database/"+dbID+"/MAIN/table/testTable/false").post(null);
			assertEquals(201, response.getStatus());
			
			ColumnRequest columnRequest = new ColumnRequest();
			columnRequest.setNewname("testColumn");
			columnRequest.setDatatype("varchar");
			columnRequest.setNullable(true);
			// Create a column
			response = getClient().path("/column/database/"+dbID+"/MAIN/table/testTable/column/testColumn/false").post(columnRequest);
			assertEquals(200, response.getStatus());
			
			// get the whole database structure
			response = getClient().path("/database/"+dbID+"/MAIN/false").get();
			assertEquals(200, response.getStatus());
			TableList tableList = (TableList)response.readEntity(TableList.class);
			@SuppressWarnings("rawtypes")
			HashMap tables = tableList.getTables();
			assertEquals(1, tables.size());
			
			
			// delete the original
			response = getClient().path("/database/"+dbID+"/MAIN").delete();
			assertEquals(200, response.getStatus());
			
			logout();
		}
	
}
