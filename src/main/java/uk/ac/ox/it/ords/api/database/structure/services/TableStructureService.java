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

import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.structure.dto.PositionRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.TableStructureServiceImpl;

public interface TableStructureService {
	
	/**
	 * Get table metadata
	 * @param database the database
	 * @param tableName the table
	 * @param staging if this applies to a staging database
	 * @return the result
	 * @throws Exception if there is a problem
	 */
	public TableList getTableMetadata ( OrdsPhysicalDatabase database, String tableName, boolean staging ) throws Exception;
	
	/**
	 * Creates a table
	 * @param database the database
	 * @param tableName the table
	 * @param staging if this applies to a staging database
	 * @throws Exception if there is a problem
	 */
	public void createNewTable ( OrdsPhysicalDatabase database, String tableName, boolean staging ) throws Exception;
	
	/**
	 * Renames a table 
	 * @param database the database
	 * @param tableName the table
	 * @param tableNewName the new name
	 * @param staging if this applies to a staging database
	 * @throws Exception if there is a problem
	 */
	public void renameTable ( OrdsPhysicalDatabase database, String tableName, String tableNewName, boolean staging ) throws Exception;
	
	
	/**
	 * Set position of table
	 * @param database the database
	 * @param positionRequest the position to set
	 * @throws Exception if there is a problem
	 */
	public void setTablePositions ( OrdsPhysicalDatabase database, PositionRequest positionRequest ) throws Exception;
	
	/**
	 * Delete table
	 * @param database the database
	 * @param tableName the table
	 * @param staging if this applies to a staging database
	 * @throws Exception if there is a problem
	 */
	public void deleteTable ( OrdsPhysicalDatabase database, String tableName, boolean staging ) throws Exception;

	/**
	 * Factory for obtaining implementations
	 */
    public static class Factory {
		private static TableStructureService provider;
	    public static TableStructureService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.it.ords.api.structure.service.TableStructureService in src/main/resources/META-INF/services
	    	// containing the classname to load as the CommentService implementation. 
	    	// By default we load the Hibernate/Postgresql implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<TableStructureService> ldr = ServiceLoader.load(TableStructureService.class);
	    		for (TableStructureService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new TableStructureServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}
}
