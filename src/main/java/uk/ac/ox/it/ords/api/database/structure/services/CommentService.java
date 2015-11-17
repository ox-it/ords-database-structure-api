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

public interface CommentService {

	// table comments
	/**
	 * Gets the comment for a table
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	public String getTableComment ( int dbId, String instance, String tableName ) throws Exception;
	
	/**
	 * Sets the comment for a table
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param comment
	 * @throws Exception
	 */
	public void setTableComment ( int dbId, String instance, String tableName, String comment ) throws Exception;
	
	
	// column comments
	/**
	 * Gets the comment for named column in a table
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param columnName
	 * @return
	 * @throws Exception
	 */
	public String getColumnComment ( int dbId, String instance, String tableName, String columnName ) throws Exception;
	
	/**
	 * Set the comment for a named column in a table
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param columnName
	 * @throws Exception
	 */
	public void setColumnComment ( int dbId, String instance, String tableName, String columnName, String comment ) throws Exception;
	
	
	
	/**
	 * Factory for obtaining implementations
	 */
    public static class Factory {
		private static CommentService provider;
	    public static CommentService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.it.ords.api.structure.service.CommentService in src/main/resources/META-INF/services
	    	// containing the classname to load as the CommentService implementation. 
	    	// By default we load the Hibernate/Postgresql implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<CommentService> ldr = ServiceLoader.load(CommentService.class);
	    		for (CommentService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	// TODO    		provider = new CommentServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}

}
