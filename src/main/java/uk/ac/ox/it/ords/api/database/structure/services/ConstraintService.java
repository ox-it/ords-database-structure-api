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

import uk.ac.ox.it.ords.api.database.structure.dto.ConstraintRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.ConstraintServiceImpl;

public interface ConstraintService {

	/**
	 * This function gets a named constraint metadata for a particular table in the database
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param constraintName
	 * @return A constraint class containing the contraint's metadata
	 * @throws Exception
	 */
	public MessageEntity getConstraint (OrdsPhysicalDatabase database, String tableName, String constraintName, boolean staging ) throws Exception;
	
	/**
	 * This function creates new constraint with the metadata contained in the ConstraintRequest class
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param newConstraint
	 * @throws Exception
	 */
	public void createConstraint (OrdsPhysicalDatabase database, String tableName, String constraintName, ConstraintRequest newConstraint, boolean staging ) throws Exception;
	
	/**
	 * Update a named constraint with the metadata in the ConstraintRequest object
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param constraintName
	 * @param constraint
	 * @throws Exception
	 */
	public void updateConstraint ( OrdsPhysicalDatabase database, String tableName, String constraintName, ConstraintRequest constraint, boolean staging ) throws Exception;
	
	/**
	 * Deletes a named constraint
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param constraint
	 * @throws Exception
	 */
	public void deleteConstraint ( OrdsPhysicalDatabase database, String tableName, String constraint, boolean staging ) throws Exception;
	
	
	
	/**
	 * Factory for obtaining implementations
	 */
    public static class Factory {
		private static ConstraintService provider;
	    public static ConstraintService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.it.ords.api.structure.service.ConstraintService in src/main/resources/META-INF/services
	    	// containing the classname to load as the ConstraintService implementation. 
	    	// By default we load the Hibernate/Postgresql implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<ConstraintService> ldr = ServiceLoader.load(ConstraintService.class);
	    		for (ConstraintService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new ConstraintServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}

}
