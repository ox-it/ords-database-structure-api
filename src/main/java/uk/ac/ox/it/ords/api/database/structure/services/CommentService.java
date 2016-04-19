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

import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.CommentServiceImpl;

public interface CommentService {

	// table comments
	/**
	 * Gets the comment for a table
	 * @param database the database
	 * @param tableName the table
	 * @param staging  whether this is a staging version
	 * @throws Exception if there is a problem getting the comment
	 * @return the comment
	 */
	public String getTableComment ( OrdsPhysicalDatabase database, String tableName, boolean staging ) throws Exception;
	
	/**
	 * Sets the comment for a table
	 * @param database the database
	 * @param tableName the table
	 * @param comment the comment to set
	 * @param staging  whether this is a staging version
	 * @throws Exception if there is a problem setting the comment
	 */
	public void setTableComment ( OrdsPhysicalDatabase database, String tableName, String comment, boolean staging ) throws Exception;
	
	
	// column comments
	
	/**
	 * Gets the comment for named column in a table
	 * @param database the database
	 * @param tableName the table
	 * @param columnName the column
	 * @param staging  whether this is a staging version
	 * @throws Exception if there is a problem getting the comment
	 * @return the comment
	 */
	public String getColumnComment ( OrdsPhysicalDatabase database, String tableName, String columnName, boolean staging ) throws Exception;
	
	/**
	 * Set the comment for a named column in a table
	 * @param database the database
	 * @param tableName the table
	 * @param columnName the column
	 * @param comment the comment to set
	 * @param staging  whether this is a staging version
	 * @throws Exception if there is a problem setting the comment
	 */
	public void setColumnComment ( OrdsPhysicalDatabase database, String tableName, String columnName, String comment, boolean staging ) throws Exception;
	
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
	    		provider = new CommentServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}

}
