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

import uk.ac.ox.it.ords.api.database.structure.dto.IndexRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.IndexServiceImpl;

public interface IndexService {
	
	/**
	 * Gets an index
	 * @param database the database
	 * @param tableName the table
	 * @param indexName the index
	 * @param staging if this applies to a staging database
	 * @return the result
	 * @throws Exception if there is a problem
	 */
	public MessageEntity getIndex ( OrdsPhysicalDatabase database, String tableName, String indexName, boolean staging ) throws Exception;
	
	/**
	 * Creates an index
	 * @param database the database
	 * @param tableName the table
	 * @param indexName the index
	 * @param newIndex the new index
	 * @param staging if this applies to a staging database
	 * @throws Exception if there is a problem
	 */
	public void createIndex ( OrdsPhysicalDatabase database, String tableName, String indexName, IndexRequest newIndex, boolean staging ) throws Exception;
	
	/**
	 * Updates an index
	 * @param database the database
	 * @param tableName the table
	 * @param indexName the index
	 * @param index the index update request
	 * @param staging if this applies to a staging database
	 * @throws Exception if there is a problem
	 */
	public void updateIndex ( OrdsPhysicalDatabase database, String tableName, String indexName, IndexRequest index, boolean staging ) throws Exception;
	
	/**
	 * Deletes an index
	 * @param database the database
	 * @param tableName the table
	 * @param indexName the index
	 * @param staging if this applies to a staging database
	 * @throws Exception if there is a problem
	 */
	public void deleteIndex ( OrdsPhysicalDatabase database, String tableName, String indexName, boolean staging ) throws Exception;

	
	/**
	 * Factory for obtaining implementations
	 */
    public static class Factory {
		private static IndexService provider;
	    public static IndexService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.it.ords.api.structure.service.IndexService in src/main/resources/META-INF/services
	    	// containing the classname to load as the IndexService implementation. 
	    	// By default we load the Hibernate/Postgresql implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<IndexService> ldr = ServiceLoader.load(IndexService.class);
	    		for (IndexService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new IndexServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}
}
