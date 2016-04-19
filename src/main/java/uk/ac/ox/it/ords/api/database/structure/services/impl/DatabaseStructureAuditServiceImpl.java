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

package uk.ac.ox.it.ords.api.database.structure.services.impl;

import org.apache.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.UnavailableSecurityManagerException;

import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureAuditService;
import uk.ac.ox.it.ords.security.model.Audit;
import uk.ac.ox.it.ords.security.services.AuditService;

public class DatabaseStructureAuditServiceImpl implements
		DatabaseStructureAuditService {
	
	static Logger log = Logger.getLogger(DatabaseStructureAuditServiceImpl.class);
	
	private String getPrincipalName(){
		try {
			if (SecurityUtils.getSubject() == null || SecurityUtils.getSubject().getPrincipal() == null) return "Unauthenticated";
			return SecurityUtils.getSubject().getPrincipal().toString();
		} catch (UnavailableSecurityManagerException e) {
			log.warn("Audit being called with no valid security context. This is probably caused by being called from unit tests");
			return "Security Manager Not Configured";
		}
	}

	@Override
	public void createDatabase(int databaseId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.CREATE_PHYSICAL_DATABASE.name());
		audit.setUserId(getPrincipalName());
		audit.setLogicalDatabaseId(databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void deleteDatabase(int databaseId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.DELETE_PHYSICAL_DATABASE.name());
		audit.setUserId(getPrincipalName());
		audit.setLogicalDatabaseId(databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void createODBCRole(int databaseId, String role) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.ADD_ODBC_ROLE.name());
		audit.setUserId(getPrincipalName());
		audit.setLogicalDatabaseId(databaseId);
		audit.setMessage("Created role:" + role);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void removeODBCRole(int databaseId, String role) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.REMOVE_ODBC_ROLE.name());
		audit.setUserId(getPrincipalName());
		audit.setMessage("Dropped role:" + role);
		audit.setLogicalDatabaseId(databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void removeODBCRoles(int databaseId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.REMOVE_ODBC_ROLE.name());
		audit.setUserId(getPrincipalName());
		audit.setMessage("Dropped all roles");
		audit.setLogicalDatabaseId(databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}

	@Override
	public void createNotAuthRecord(String request) {
		Audit audit= new Audit();
		audit.setAuditType(Audit.AuditType.GENERIC_NOTAUTH.name());
		audit.setUserId(getPrincipalName());
		audit.setMessage(request);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void createNotAuthRecord(String request, int logicalDatabaseId) {
		Audit audit= new Audit();
		audit.setAuditType(Audit.AuditType.GENERIC_NOTAUTH.name());
		audit.setUserId(getPrincipalName());
		audit.setMessage(request);
		audit.setLogicalDatabaseId(logicalDatabaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}

}
