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

package uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate;

import org.apache.shiro.SecurityUtils;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissionSets;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureRoleService;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.model.UserRole;

public class DatabaseStructureRoleServiceImpl
		implements
			DatabaseStructureRoleService {

	private static Logger log = LoggerFactory.getLogger(DatabaseStructureRoleServiceImpl.class);
	protected SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public DatabaseStructureRoleServiceImpl() {
		setSessionFactory (HibernateUtils.getSessionFactory());
	}

	@Override
	public void createInitialPermissions(int logicalDatabaseId) throws Exception {
		Session session = this.sessionFactory.openSession();
		
		try {
			session.beginTransaction();
			
			//
			// Assign the principal to the owner role
			//
			UserRole owner = new UserRole();
			owner.setPrincipalName(SecurityUtils.getSubject().getPrincipal().toString());
			owner.setRole(getPrivateUserRole("databaseowner", logicalDatabaseId));
			session.save(owner);
			session.getTransaction().commit();
			
			//
			// Create the permissions for roles associated with the project
			//
			createPermissionsForDatabase(logicalDatabaseId);

		} catch (HibernateException e) {
			log.error("Error creating Project", e);
			session.getTransaction().rollback();
			throw new Exception("Cannot create project",e);
		}
		finally {
			session.close();
		}

	}

	private String getPrivateUserRole(String role, int projectId) {
		if (role.contains("_")) return role;
		return role+"_"+projectId;
	}
	
	/**
	 * Each project has a set of roles and permissions
	 * associated with it.
	 * 
	 * By default these are:
	 * 
	 *   owner_{projectId}
	 *   contributor_{projectId}
	 *   viewer_{projectId}
	 *   
	 * @param projectId
	 * @throws Exception 
	 */
	private void createPermissionsForDatabase(int logicalDatabaseId) throws Exception{
		//
		// Owner
		//
		String ownerRole = "databaseowner_"+logicalDatabaseId;
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForOwner(logicalDatabaseId)){
			createPermission(ownerRole, permission);			
		}

		//
		// Contributor
		//
		String contributorRole = "databasecontributor_"+logicalDatabaseId;
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForContributor(logicalDatabaseId)){
			createPermission(contributorRole, permission);			
		}

		//
		// Viewer
		//
		String viewerRole = "databaseviewer_"+logicalDatabaseId;
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForViewer(logicalDatabaseId)){
			createPermission(viewerRole, permission);			
		}
	}

	/**
	 * Creates a permission
	 * @param role the role 
	 * @param permissionString the permission
	 * @throws Exception if there is a problem creating the permission
	 */
	protected void createPermission(String role, String permissionString) throws Exception{
		Session session = this.sessionFactory.openSession();
		try {
			session.beginTransaction();
			Permission permission = new Permission();
			permission.setRole(role);
			permission.setPermission(permissionString);
			session.save(permission);
			session.getTransaction().commit();
		} catch (Exception e) {
			log.error("Error creating permission", e);
			session.getTransaction().rollback();
			throw new Exception("Cannot create permission",e);
		}
		finally {
			session.close();
		}

	}

}
