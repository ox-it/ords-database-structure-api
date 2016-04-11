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

import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.StructureODBCServiceImpl;
import uk.ac.ox.it.ords.security.services.ODBCService;

public interface StructureODBCService extends ODBCService {
	
	/**
	 * Create a read-only ODBC connection role
	 * @param role the role to create
	 * @param odbcPassword the ODBC password
	 * @param database the database
	 * @param databaseName the database name
	 * @throws Exception if there is a problem adding the role
	 */
	public abstract void addReadOnlyOdbcUserToDatabase(String role, String odbcPassword,
			OrdsPhysicalDatabase database, String databaseName)
			throws Exception;

	public abstract void addOdbcUserToDatabase(String role, String password, OrdsPhysicalDatabase database, String databaseName) throws Exception;

	public abstract void removeOdbcUserFromDatabase(String role, OrdsPhysicalDatabase database, String databaseName)  throws Exception;

	public abstract String getODBCUserName(String databaseName) throws Exception;
		
	public abstract void removeAllODBCRolesFromDatabase(OrdsPhysicalDatabase database)
			throws Exception;
	
	public abstract List<String> getAllODBCRolesForDatabase(String databaseServer, String databaseName) throws Exception;
		
	/**
	 * Factory for obtaining implementations
	 */
    public static class Factory {
		private static StructureODBCService provider;
	    public static StructureODBCService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.oucs.ords.utilities.csv in src/main/resources/META-INF/services
	    	// containing the classname to load as the CsvService implementation. 
	    	// By default we load the Hibernate implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<StructureODBCService> ldr = ServiceLoader.load(StructureODBCService.class);
	    		for (StructureODBCService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new StructureODBCServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}
}
