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
import uk.ac.ox.it.ords.api.database.structure.dto.ConstraintRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;

public class ContraintTest extends AbstractDatabaseTestRunner{
	
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
	public void addConstraintWithNoColumnsSpecified(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());

		// Add constraints
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.UNIQUE, new String[0], null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/uk/false").post(constraint);
		assertEquals(400, response.getStatus());
		
		constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.PRIMARY, new String[0], null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/uk/false").post(constraint);
		assertEquals(400, response.getStatus());
		
		constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.FOREIGN, new String[0], null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/uk/false").post(constraint);
		assertEquals(400, response.getStatus());
	}

	@Test
	public void addConstraintUnauth(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());

		logout();

		// Add constraints
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.UNIQUE, new String[]{"testColumn"}, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/uk/false").post(constraint);
		assertEquals(403, response.getStatus());
	}
	
	@Test
	public void getConstraintUnauth(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());

		// Add constraints
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.UNIQUE, new String[]{"testColumn"}, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/uk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		logout();
		
		assertEquals(403, getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/uk_1/false").get().getStatus());
	}
	
	@Test
	public void addConstraintWithNoTable(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		String[] columns = {"testColumn"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.UNIQUE, columns, null, null);
		Response response = getClient().path("/"+physicalDatabaseId+"/table/banana/constraint/uk_1/false").post(constraint);
		assertEquals(404, response.getStatus());
	}
	
	@Test
	public void addUniqueKey(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());

		// Add a UNIQUE constraint
		String[] columns = {"testColumn"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.UNIQUE, columns, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/uk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		// Get PK - note how ORDS adds a number on to ensure constraints are uniquely identified
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/uk_1/false").get();
		assertEquals(200, response.getStatus());
		
		// Remove PK
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/uk_1/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();

	}
	
	@Test
	public void addPrimaryKey(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());

		// Add a PK constraint
		String[] columns = {"testColumn"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.PRIMARY, columns, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		// Get PK - note how ORDS adds a number on to ensure constraints are uniquely identified
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").get();
		assertEquals(200, response.getStatus());
		
		// Remove PK
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();

	}
	
	@Test
	public void updateConstraint(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());

		// Add a PK constraint
		String[] columns = {"testColumn"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.PRIMARY, columns, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		// Get PK - note how ORDS adds a number on to ensure constraints are uniquely identified
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").get();
		assertEquals(200, response.getStatus());
		
		// Update PK
		constraint.setNewname("primary_key");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").put(constraint);
		assertEquals(200, response.getStatus());
		
		// Remove PK
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/primary_key/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();

	}
	
	@Test
	public void updateConstraintNonExisting(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
	
		// Update PK
		String[] columns = {"testColumn"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.PRIMARY, columns, null, null);
		Response response = getClient().path("/99999/table/testtable/constraint/pk_1/false").put(constraint);
		assertEquals(404, response.getStatus());
		
		logout();
	}
	
	@Test
	public void updateConstraintUnauth(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());

		// Add a PK constraint
		String[] columns = {"testColumn"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.PRIMARY, columns, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		// Get PK - note how ORDS adds a number on to ensure constraints are uniquely identified
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").get();
		assertEquals(200, response.getStatus());
		
		// Update PK
		logout();
		loginUsingSSO("pinga@penguins.com","");
		constraint.setNewname("primary_key");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").put(constraint);
		assertEquals(403, response.getStatus());
		logout();
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		// Remove PK
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();

	}
	
	@Test
	public void deleteConstraint(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Check column exists
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").get();
		assertEquals(200, response.getStatus());

		// Add a PK constraint
		String[] columns = {"testColumn"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.PRIMARY, columns, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		// Get PK - note how ORDS adds a number on to ensure constraints are uniquely identified
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").get();
		assertEquals(200, response.getStatus());
		
		// Remove PK unauth
		logout();
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").delete();
		assertEquals(403, response.getStatus());
		
		// Remove PK wrong id
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_9/false").delete();
		assertEquals(404, response.getStatus());
		
		logout();

	}
	
	@Test
	public void addForeignKey(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Create another column
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn2/false").post(column1);
		assertEquals(201, response.getStatus());

		// Add a PK constraint
		String[] columns = {"testColumn"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.PRIMARY, columns, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		// Get PK - note how ORDS adds a number on to ensure constraints are uniquely identified
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").get();
		assertEquals(200, response.getStatus());
		
		// Add an FK (self-join in this case)
		String[] columns2 = {"testColumn2"};
		constraint = this.buildConstraintRequest("fk", DatabaseTest.constraint_type.FOREIGN, columns2, "testtable", "testColumn");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/fk/false").post(constraint);
		assertEquals(201, response.getStatus());

		// Remove FK
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/fk_2/false").delete();
		assertEquals(200, response.getStatus());
		
		// Remove PK
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();

	}
	
	@Test
	public void addForeignKeyWithMissingInfo(){
		
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		// build a column
		ColumnRequest column1 = this.buildColumnRequest("testColumn", "varchar", null, true, false);
		
		// Create a column
		Response response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn/false").post(column1);
		assertEquals(201, response.getStatus());
		
		// Create another column
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/column/testColumn2/false").post(column1);
		assertEquals(201, response.getStatus());

		// Add a PK constraint
		String[] columns = {"testColumn"};
		ConstraintRequest constraint = this.buildConstraintRequest("pk", DatabaseTest.constraint_type.PRIMARY, columns, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk/false").post(constraint);
		assertEquals(201, response.getStatus());
		
		// Add an FK that doesn't reference anything
		String[] columns2 = {"testColumn2"};
		constraint = this.buildConstraintRequest("fk", DatabaseTest.constraint_type.FOREIGN, columns2, null, null);
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/fk/false").post(constraint);
		assertEquals(400, response.getStatus());
		
		// Add an FK that has multiple source columns
		String[] columns3 = {"testColumn", "testColumn2"};
		constraint = this.buildConstraintRequest("fk", DatabaseTest.constraint_type.FOREIGN, columns3, "testtable", "testColumn");
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/fk/false").post(constraint);
		assertEquals(400, response.getStatus());
		
		// TODO Add an FK that points to a non-existant ref table
		//constraint = this.buildConstraintRequest("fk", DatabaseTest.constraint_type.FOREIGN, columns2, "testtablenope", "testColumn");
		//response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/fk/false").post(constraint);
		//assertEquals(400, response.getStatus());
		
		// Remove PK
		response = getClient().path("/"+physicalDatabaseId+"/table/testtable/constraint/pk_1/false").delete();
		assertEquals(200, response.getStatus());
		
		logout();

	}
	
}
