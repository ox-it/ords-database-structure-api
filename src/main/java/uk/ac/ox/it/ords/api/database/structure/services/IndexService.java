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

import uk.ac.ox.it.ords.api.database.structure.metadata.IndexRequest;

public interface IndexService {
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param indexName
	 * @return
	 * @throws Exception
	 */
	public IndexRequest getIndex ( int dbId, String instance, String tableName, String indexName ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param newIndex
	 * @throws Exception
	 */
	public void createIndex ( int dbId, String instance, String tableName, IndexRequest newIndex ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param indexName
	 * @param index
	 * @throws Exception
	 */
	public void updateIndex ( int dbId, String instance, String tableName, String indexName, IndexRequest index ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param index
	 * @throws Exception
	 */
	public void deleteIndex ( int dbId, String instance, String tableName, String indexName ) throws Exception;

	
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
	// TODO    		provider = new IndexServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}
}
