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
	 * @return List of OrdsPhysicalDatabase instances for the current user
	 * @throws Exception if there is a problem obtaining the databases
	 */
	public List<OrdsPhysicalDatabase> getDatabaseList ( ) throws Exception;
	
	/**
	 * Creates a new database for the current user principal. Passing null for newDatabase
	 * uses default values and names for the database.
	 * @param databaseDTO the database request 
	 * @return the new database
	 * @throws Exception if there is a problem creating the database
	 */
	public OrdsPhysicalDatabase createNewDatabase (DatabaseRequest databaseDTO) throws Exception;

	/**
	 * Creates a new database for the current user principal. Passing null for newDatabase
	 * uses default values and names for the database.
	 * @param databaseDTO the database request
	 * @param sourceId the existing database to use as a template
	 * @return the new database
	 * @throws Exception if there is a problem creating the database
	 */
	public OrdsPhysicalDatabase createNewDatabaseFromExisting ( int sourceId , DatabaseRequest databaseDTO) throws Exception;

	
	/**
	 * Checks if the specified database exists
	 * @param dbId the database id
	 * @param staging if this is a staging database
	 * @return true if the database exists
	 * @throws Exception if there is a problem carrying out the check
	 */
	public boolean checkDatabaseExists( int dbId, boolean staging ) throws Exception;
	
	/**
	 * Gets the metadata for a specific database
	 * @param dbId the database id
	 * @return the database metadata
	 * @throws Exception if there is a problem obtaining the metadata
	 */
	public OrdsPhysicalDatabase getDatabaseMetaData ( int dbId ) throws Exception;
	
	/**
	 * Gets a table list object for the database which gives a complete breakdown the tables
	 * and the columns indexes etc...
	 * @param idbId the database id
	 * @param staging whether this relates to a staging database
	 * @return the Table List
	 * @throws Exception if there is a problem obtaining the metadata
	 */
	public TableList getDatabaseTableList ( int idbId, boolean staging ) throws Exception;
	
	/**
	 * Creates a staging database for editing
	 * @param dbId the database to create a staging instance for
	 * @return the name of the database? TODO check and possibly change this method signature
	 * @throws Exception if there is a problem creating the staging instance
	 */
	public String createNewStagingDatabase ( int dbId ) throws Exception;
	
	/**
	 * Merges the staging version of the database with the actual, if the staging version exists
	 * This is the save operation
	 * @param mainId the database
	 * @throws Exception if there is a problem with the merge
	 */
	public void mergeStagingToActual ( int mainId ) throws Exception;
	
	/**
	 * Deletes a specific database
	 * @param dbId the database
	 * @param staging whether this relates to a staging database
	 * @throws Exception if there is a problem deleting the database
	 */
	public void deleteDatabase ( int dbId, boolean staging ) throws Exception;
	
	/**
	 * Deletes target and replaces it with the source
	 * @param source the source database
	 * @param target the target database
	 * @return the merged database
	 * @throws Exception if there is a problem with the merge operation
	 */
	public OrdsPhysicalDatabase mergeInstanceToMain( OrdsPhysicalDatabase source, OrdsPhysicalDatabase target ) throws Exception;
	
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
