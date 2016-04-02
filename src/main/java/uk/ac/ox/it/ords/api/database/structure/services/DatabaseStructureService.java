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

package uk.ac.ox.it.ords.api.database.structure.services;

import java.util.List;
import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.*;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.DatabaseStructureServiceImpl;

public interface DatabaseStructureService {
	
	
	public void init() throws Exception;
	
	/**
	 * Gets a list of all databases for the current user principal
	 * @return
	 * @throws Exception
	 */
	public List<OrdsPhysicalDatabase> getDatabaseList ( ) throws Exception;
	
	
	
	/**
	 * Creates a new database for the current user principal. Passing null for newDatabase
	 * uses default values and names for the database.
	 * @param newDatabase
	 * @return
	 * @throws Exception
	 */
	public OrdsPhysicalDatabase createNewDatabase ( int logicalDBId , DatabaseRequest databaseDTO, String instance) throws Exception;
	
	/**
	 * Checks if the specified database exists
	 * @param dbId
	 * @param instance
	 * @param staging
	 * @return
	 * @throws Exception
	 */
	public boolean checkDatabaseExists( int dbId, String instance, boolean staging ) throws Exception;
	
	/**
	 * Gets the metadata for a specific database
	 * @param dbId
	 * @param instanceId
	 * @return
	 * @throws Exception
	 */
	public OrdsPhysicalDatabase getDatabaseMetaData ( int dbId, String instance ) throws Exception;
	
	/**
	 * Gets the logical database (group) for a specific database
	 * @param dbId
	 * @return
	 * @throws Exception
	 */
	public OrdsDB getLogicalDatabase (int dbId) throws Exception;
	
	/**
	 * Gets a table list object for the database which gives a complete breakdown the tables
	 * and the columns indexes etc...
	 * @param idbId
	 * @param instance
	 * @return
	 * @throws Exception
	 */
	public TableList getDatabaseTableList ( int idbId, String instance, boolean staging ) throws Exception;
	
	/**
	 * Creates a staging database for editing
	 * @param dbId
	 * @param instance
	 * @throws Exception
	 */
	public String createNewStagingDatabase ( int dbId, String instance ) throws Exception;
	
	/**
	 * Updates the metadata for the staging database
	 * @param update
	 * @throws Exception
	 */
	public void updateStagingDatabase ( int dbId, OrdsPhysicalDatabase update ) throws Exception;
	
	/**
	 * Merges the staging version of the database with the actual, if the staging version exists
	 * This is the save operation
	 * @param dbId
	 * @throws Exception
	 */
	public void mergeStagingToActual ( int dbId, String instance ) throws Exception;
	
	/**
	 * Deletes a specific database
	 * @param dbId
	 * @throws Exception
	 */
	public void deleteDatabase ( int dbId, String instance, boolean Staging ) throws Exception;
	
	/**
	 * Deletes main and replaces it with the instance
	 * @param dbId
	 * @param instance
	 * @return
	 * @throws Exception
	 */
	public OrdsPhysicalDatabase mergeInstanceToMain( int dbId, String instance ) throws Exception;
	
	/**
	 * Factory for obtaining implementations
	 */
    public static class Factory {
		private static DatabaseStructureService provider;
	    public static DatabaseStructureService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.it.ords.api.structure.service.DatabaseStructureService in src/main/resources/META-INF/services
	    	// containing the classname to load as the DatabaseStructureService implementation. 
	    	// By default we load the Hibernate/Postgresql implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<DatabaseStructureService> ldr = ServiceLoader.load(DatabaseStructureService.class);
	    		for (DatabaseStructureService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new DatabaseStructureServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}

	
}
