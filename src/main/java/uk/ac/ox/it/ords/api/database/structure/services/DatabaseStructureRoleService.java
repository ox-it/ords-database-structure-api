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

import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.DatabaseStructureRoleServiceImpl;
import uk.ac.ox.it.ords.security.model.UserRole;

public interface DatabaseStructureRoleService {

	/**
	 * Update a role
	 * @param userRole
	 */
	public void updateDatabseRole(UserRole userRole, int dbId) throws Exception;
	
	/**
	 * Gets the project owner, if any
	 */
	public UserRole getDatabaseOwner(int dbId) throws Exception;
	
	/**
	 * Create the Owner role and their permissions; called once when a new project is created
	 * @param groupId
	 * @throws Exception
	 */
	public void createInitialPermissions(int groupId) throws Exception;
	
	/**
	 * Delete all the permissions and roles associated with a project; called once when a project is deleted
	 * @param groupId
	 * @throws Exception
	 */
	public void deletePermissions(int dbId) throws Exception;
	
	/**
	 * Return all the UserRoles that match the pattern of the project
	 * @param groupId
	 * @return a List of UserRole objects
	 * @throws Exception
	 */
	public List<UserRole> getUserRolesForDatabase(int dbId) throws Exception;
	

	/**
	 * Return the specified UserRole instance
	 * @param roleId 
	 * @return the UserRole specified, or null if there is no match
	 * @throws Exception
	 */
	public UserRole getUserRole(int roleId) throws Exception;
	
	/**
	 * Create the UserRole 
	 * @param groupId
	 * @param userRole
	 * @return the UserRole that has been persisted
	 * @throws Exception
	 */
	public UserRole addUserRoleToDatabase(int dbId, UserRole userRole) throws Exception;
	
	/**
	 * Remove the UserRole
	 * @param projectid
	 * @param roleId
	 * @throws Exception
	 */
	public void removeUserFromRoleInDatabase(int dbId, int roleId) throws Exception;	

	/**
	 * The enumeration of valid UserRole types
	 */
    public enum DatabaseRole {
        owner, databaseadministrator, contributor, viewer, deleted
    };

	   public static class Factory {
			private static DatabaseStructureRoleService provider;
		    public static DatabaseStructureRoleService getInstance() {
		    	//
		    	// Use the service loader to load an implementation if one is available
		    	// Place a file called uk.ac.ox.oucs.ords.utilities.csv in src/main/resources/META-INF/services
		    	// containing the classname to load as the CsvService implementation. 
		    	// By default we load the Hibernate implementation.
		    	//
		    	if (provider == null){
		    		ServiceLoader<DatabaseStructureRoleService> ldr = ServiceLoader.load(DatabaseStructureRoleService.class);
		    		for (DatabaseStructureRoleService service : ldr) {
		    			// We are only expecting one
		    			provider = service;
		    		}
		    	}
		    	//
		    	// If no service provider is found, use the default
		    	//
		    	if (provider == null){
		    		provider = new DatabaseStructureRoleServiceImpl();
		    	}
		    	
		    	return provider;
		    }
	   }

	String getPrivateUserRole(String role, int projectId);

	String getPublicUserRole(String role);

}
