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
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.TableStructureServiceImpl;

public interface TableStructureService {
	
	/**
	 * 
	 * @param dbID
	 * @param instance
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	public TableList getTableMetadata ( int dbID, String instance, String tableName, boolean stagin ) throws Exception;
	
	/**
	 * 
	 * @param dbID
	 * @param instance
	 * @param tableName
	 * @throws Exception
	 */
	public void createNewTable ( int dbID, String instance, String tableName, boolean staging ) throws Exception;
	
	/**
	 * 
	 * @param dbID
	 * @param instance
	 * @param tableName
	 * @param tableNewName
	 * @throws Exception
	 */
	public void renameTable ( int dbID, String instance, String tableName, String tableNewName, boolean staging ) throws Exception;
	
	
	/**
	 * 
	 * @param dbID
	 * @param instance
	 * @param positionRequest
	 */
	public void setTablePositions ( int dbID, String instance, PositionRequest positionRequest ) throws Exception;
	
	/**
	 * 
	 * @param dbID
	 * @param instance
	 * @param tableName
	 * @throws Exception
	 */
	public void deleteTable ( int dbID, String instance, String tableName, boolean staging ) throws Exception;

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
