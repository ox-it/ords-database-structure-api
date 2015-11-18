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

import java.util.List;

import org.apache.shiro.SecurityUtils;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissionSets;
import uk.ac.ox.it.ords.api.database.structure.server.ValidationException;
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
	public void updateDatabseRole(UserRole userRole, int dbId) throws Exception {
		userRole.setRole(getPrivateUserRole(userRole.getRole(), dbId));
		Session session = this.sessionFactory.getCurrentSession();
		try {
			session.beginTransaction();
			session.update(userRole);
			session.getTransaction().commit();
		} catch (HibernateException e) {
			log.error("Error update UserRole", e);
			session.getTransaction().rollback();
			throw new Exception("Cannot update UserRole",e);
		} finally {
			  HibernateUtils.closeSession();
		}

	}

	@Override
	public UserRole getDatabaseOwner(int dbId) throws Exception {
		List<UserRole> userRoles = getUserRolesForDatabase(dbId);
		for (UserRole userRole : userRoles){
			if (userRole.getRole().startsWith("owner_")) return userRole;
		}
		return null;
	}

	@Override
	public void createInitialPermissions(int dbId) throws Exception {
		Session session = this.sessionFactory.getCurrentSession();
		
		try {
			session.beginTransaction();
			//
			// Assign the principal to the owner role
			//
			UserRole owner = new UserRole();
			owner.setPrincipalName(SecurityUtils.getSubject().getPrincipal().toString());
			owner.setRole(getPrivateUserRole("owner", dbId));
			session.save(owner);
			session.getTransaction().commit();
			//
			// Create the permissions for roles associated with the project
			//
			createPermissionsForDatabase(dbId);

		} catch (HibernateException e) {
			log.error("Error creating Project", e);
			session.getTransaction().rollback();
			throw new Exception("Cannot create project",e);
		} finally {
			  HibernateUtils.closeSession();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void deletePermissions(int dbId) throws Exception {
		Session session = this.sessionFactory.getCurrentSession();
		try {
			session.beginTransaction();
			//
			// Get the roles associated with the project
			//
			List<UserRole> owners = session.createCriteria(UserRole.class)
					.add(Restrictions.eq("role", "owner_"+dbId)).list();
			List<UserRole> contributors = session.createCriteria(UserRole.class)
					.add(Restrictions.eq("role", "contributor_"+dbId)).list();	
			List<UserRole> viewers = session.createCriteria(UserRole.class)
					.add(Restrictions.eq("role", "viewer_"+dbId)).list();

			//
			// Delete all the permissions for each role, and then each role
			//
			for (UserRole owner : owners){
				deletePermissionsForRole(session, owner);
				session.delete(owner);
			}
			for (UserRole contributor : contributors){
				deletePermissionsForRole(session, contributor);
				session.delete(contributor);
			}
			for (UserRole viewer : viewers){
				deletePermissionsForRole(session, viewer);
				session.delete(viewer);
			}
			session.getTransaction().commit();

		} catch (HibernateException e) {
			log.error("Error removing roles and permissions", e);
			session.getTransaction().rollback();
			throw new Exception("Cannot revoke project permissions",e);
		} finally {
			  HibernateUtils.closeSession();
		}
	}
	
	@SuppressWarnings("unchecked")
	private void deletePermissionsForRole(Session session, UserRole role){
		List<Permission> permissions = session.createCriteria(Permission.class)
				.add(Restrictions.eq("role", role.getRole())).list();
		for (Permission permission : permissions){
			session.delete(permission);
		}
	}

	@Override
	public List<UserRole> getUserRolesForDatabase(int dbId) throws Exception {
		Session session = this.sessionFactory.getCurrentSession();
		try {
			session.beginTransaction();
			@SuppressWarnings("unchecked")
			List<UserRole> userRoles = session.createCriteria(UserRole.class)
					.add(Restrictions.like("role", "%_"+dbId))
					.list();
			session.getTransaction().commit();
			return userRoles;
		} catch (Exception e) {
			session.getTransaction().rollback();
			throw e;
		} finally {
			  HibernateUtils.closeSession();
		}

	}

	@Override
	public UserRole getUserRole(int roleId) throws Exception {
		Session session = this.sessionFactory.getCurrentSession();
		try {
			session.beginTransaction();
			UserRole userRole = (UserRole) session.get(UserRole.class, roleId);
			session.getTransaction().commit();
			return userRole;
		} catch (Exception e) {
			session.getTransaction().rollback();
			throw e;
		} finally {
			  HibernateUtils.closeSession();
		}

	}

	@Override
	public UserRole addUserRoleToDatabase(int dbId, UserRole userRole)
			throws Exception {
		Session session = this.sessionFactory.getCurrentSession();
		try {
			session.beginTransaction();
			validate(userRole);
			String projectRole = getPrivateUserRole(userRole.getRole(), dbId);
			userRole.setRole(projectRole);
			session.save(userRole);
			session.getTransaction().commit();
			//ProjectAuditService.Factory.getInstance().createProjectUser(userRole, projectId);
			return userRole;
		} catch (HibernateException e) {
			log.error("Error creating user role", e);
			session.getTransaction().rollback();
			throw new Exception("Cannot create user role",e);
		} finally {
			  HibernateUtils.closeSession();
		}

	}

	@Override
	public void removeUserFromRoleInDatabase(int dbId, int roleId)
			throws Exception {
		//
		// First, obtain the UserRole
		//
		UserRole userRole = getUserRole(roleId);
		if (userRole == null) throw new Exception("Cannot find user role");
		//
		// Lets check that the role contains the project id
		//
		if(!userRole.getRole().endsWith(String.valueOf(dbId))){
			throw new ValidationException("Attempt to remove role via another project");
		}
		removeUserRole(userRole, dbId);

	}
	
	protected void removeUserRole(UserRole userRole, int dbId) throws Exception{
		Session session = this.sessionFactory.getCurrentSession();
		try {
			session.beginTransaction();
			session.delete(userRole);
			session.getTransaction().commit();
			//ProjectAuditService.Factory.getInstance().deleteProjectRole(userRole, projectId);
		} catch (Exception e) {
			session.getTransaction().rollback();
			log.error("Cannot find user role", e);
			throw new Exception("Cannot find user role",e);
		} finally {
			  HibernateUtils.closeSession();
		}
	}


	/* (non-Javadoc)
	 * @see uk.ac.ox.it.ords.api.project.services.ProjectRoleService#getPublicUserRole(java.lang.String)
	 */
	@Override
	public String getPublicUserRole(String role) {
		if (!role.contains("_")) return role;
		return role.split("_")[0];
	}
	
	

	@Override
	public String getPrivateUserRole(String role, int projectId) {
		if (role.contains("_")) return role;
		return role+"_"+projectId;
	}

	
    public enum GroupRole {
        owner, contributor, viewer, deleted
    };

	/**
	 * @param userRole
	 * @return
	 * @throws ValidationException
	 */
	public boolean validate(UserRole userRole) throws ValidationException{
		if (userRole == null) throw new ValidationException("Invalid role");
		if (userRole.getPrincipalName() == null) throw new ValidationException("No user principal set for role");
		if (userRole.getRole() == null) throw new ValidationException("No role set");
		if (!isValidRole(userRole.getRole())) throw new ValidationException("Invalid role type");
		return true;
	}

	private boolean isValidRole(String role){
		for (GroupRole projectRole : GroupRole.values()){
			if (projectRole.name().equals(role)) return true;
		}
		return false;
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
	private void createPermissionsForDatabase(int dbId) throws Exception{
		//
		// Owner
		//
		String ownerRole = "owner_"+dbId;
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForOwner(dbId)){
			createPermission(ownerRole, permission);			
		}

		//
		// Contributor
		//
		String contributorRole = "contributor_"+dbId;
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForContributor(dbId)){
			createPermission(contributorRole, permission);			
		}

		//
		// Viewer
		//
		String viewerRole = "viewer_"+dbId;
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForViewer(dbId)){
			createPermission(viewerRole, permission);			
		}
	}

	/**
	 * @param role
	 * @param permissionString
	 * @throws Exception
	 */
	protected void createPermission(String role, String permissionString) throws Exception{
		Session session = this.sessionFactory.getCurrentSession();
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
		} finally {
			  HibernateUtils.closeSession();
		}
	}

}
