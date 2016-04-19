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

package uk.ac.ox.it.ords.api.database.structure.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.math.BigInteger;
import java.security.SecureRandom;

import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.structure.dto.OdbcResponse;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissions;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureAuditService;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.StructureODBCService;

/**
 * API for requesting and revoking ODBC access
 */
@Api(value="ODBC")
@Path("/")
public class Odbc {
	
	//
	// We use this to generate random passcodes
	//
	private SecureRandom random = new SecureRandom();
	
	/**
	 * Request ODBC access to a database for the current subject. This will generate new credentials, which
	 * are returned to the requestor, but are not stored anywhere within ORDS.
	 * @param id the database 
	 * @return an ODBCResponse containing all the connection details needed by a client to connect to the database by ODBC
	 * @throws Exception if there is a problem obtaining ODBC access
	 */
	@ApiOperation(
			value="Create ODBC connection details", 
			notes="Returns a new set of connection details for this database for the current user", 
			response = uk.ac.ox.it.ords.api.database.structure.dto.OdbcResponse.class
			)
	@POST
	@Path("{id}/odbc/")
	@Produces( MediaType.APPLICATION_JSON )
	public Response addOdbcRoleForCurrentUser(
			@PathParam("id") int id
		) throws Exception{
		
		//
		// Check we have a logged in user
		//
		if (SecurityUtils.getSubject() == null || !SecurityUtils.getSubject().isAuthenticated()){
			
			//
			// If we don't, audit the attempt, and return a 401
			//
			DatabaseStructureAuditService.Factory.getInstance().createNotAuthRecord(String.format("POST structure/%s/odbc Not Authenticated", id));
			return Response.status(401).build();
		}
		
		//
		// Obtain the database referred to
		//
		OrdsPhysicalDatabase database = null;
		
		//
		// Check that the database exists
		//
		try {
			database = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(id);
		} catch (Exception e) {
			return Response.status(404).build();
		}
		if (database == null){
			return Response.status(404).build();
		}
		
		//
		// Get the name of the database within PostgreSQL
		//
		String databaseName = database.getDbConsumedName();
		
		//
		// Check ODBC is enabled for this database for this user
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_REQUEST_ODBC_ACCESS(database.getLogicalDatabaseId()))){

			//
			// If not, audit the attempt and return 403
			//
			DatabaseStructureAuditService.Factory.getInstance().createNotAuthRecord(String.format("POST structure/%s/odbc Not Permitted", id), database.getLogicalDatabaseId());
			return Response.status(403).build();
		}

		//
		// Generate a random password. We return this from the service, but never store it.
		//
		String password = new BigInteger(130, random).toString(32);
		
		//
		// Check permissions and create the appropriate role
		//
		if (SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_MODIFY(database.getLogicalDatabaseId()))){
			
			//
			// User has Modify rights, so create a read-write ODBC role
			//
			StructureODBCService.Factory.getInstance().addOdbcUserToDatabase(StructureODBCService.Factory.getInstance().getODBCUserName(databaseName), password, database, databaseName);	

		} else if (SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_VIEW(database.getLogicalDatabaseId()))){
			
			//
			// User has View rights, so create a read-only ODBC role
			//
			StructureODBCService.Factory.getInstance().addReadOnlyOdbcUserToDatabase(StructureODBCService.Factory.getInstance().getODBCUserName(databaseName), password, database, databaseName);

		} else {
			
			//
			// User has no permissions for this database so cannot have an ODBC role
			//
			DatabaseStructureAuditService.Factory.getInstance().createNotAuthRecord(String.format("POST structure/%s/odbc No Permitted Role", id), database.getLogicalDatabaseId());
			return Response.status(403).build();
		}
		
		
		//
		// We return the generated password. We could alternatively email it to the user.
		//
		OdbcResponse response = new OdbcResponse();
		response.setServer(database.getDatabaseServer());
		response.setDatabase(databaseName);
		response.setPassword(password);
		response.setUsername(StructureODBCService.Factory.getInstance().getODBCUserName(databaseName));
		
		//
		// Add an audit record for this event
		//
		DatabaseStructureAuditService.Factory.getInstance().createODBCRole(database.getLogicalDatabaseId(), response.getUsername());
		
		return Response.ok(response).build();
	}
	
	/**
	 * Revokes ODBC access on a database for all roles
	 * @param id the database id
	 * @return Response containing a status code
	 * @throws Exception if there is a problem deleting ODBC access
	 */
	@DELETE
	@Path("{id}/odbc/")
	@Produces( MediaType.APPLICATION_JSON )
	public Response removeAllOdbcRoles(
			@PathParam("id") int id
			) throws Exception{
		
		//
		// Check we have a logged in user
		//
		if (SecurityUtils.getSubject() == null || !SecurityUtils.getSubject().isAuthenticated()){
			//
			// If not, audit the attempt and return 401
			//
			DatabaseStructureAuditService.Factory.getInstance().createNotAuthRecord(String.format("DELETE structure/%s/odbc Not Authenticated", id));
			return Response.status(401).build();
		}
		
		//
		// Obtain the database referred to
		//
		OrdsPhysicalDatabase database = null;
		
		//
		// Check that it exists
		//
		try {
			database = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(id);
		} catch (Exception e) {
			return Response.status(404).build();
		}
		if (database == null){
			return Response.status(404).build();
		}

		//
		// Check permission - we want more than just "modify" permission for this. We may want to
		// define a specific permission in future, but DATABASE_DELETE seems sufficient.
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_DELETE(database.getLogicalDatabaseId()))){
			
			//
			// If not permitted, create an audit record and return 403
			//
			DatabaseStructureAuditService.Factory.getInstance().createNotAuthRecord(String.format("DELETE structure/%s/odbc Not permitted", id), database.getLogicalDatabaseId());

			return Response.status(403).build();			
		}
		
		//
		// Remove all roles
		//
		StructureODBCService.Factory.getInstance().removeAllODBCRolesFromDatabase(database);
		
		//
		// Add an audit record for the change
		//
		DatabaseStructureAuditService.Factory.getInstance().removeODBCRoles(database.getLogicalDatabaseId());
		
		return Response.ok().build();
	}

	
	/**
	 * Revokes ODBC access on a database for a role
	 * @param id the database id
	 * @param role the name of the role to revoke
	 * @return a Response containing a status code
	 * @throws Exception if there is a problem removing the role
	 */
	@DELETE
	@Path("{id}/odbc/{role}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response removeOdbcRole(
			@PathParam("id") int id,
			@PathParam("role") String role
			) throws Exception{

		
		//
		// Check we have a logged in user
		//
		if (SecurityUtils.getSubject() == null || !SecurityUtils.getSubject().isAuthenticated()){
			
			//
			// If not, add an audit record for the attempt, and return 401
			//
			DatabaseStructureAuditService.Factory.getInstance().createNotAuthRecord(String.format("DELETE structure/%s/odbc/%s Not authenticated", id, role));
			return Response.status(401).build();
		}
		
		//
		// Obtain the database referred to
		//
		OrdsPhysicalDatabase database = null;
		
		//
		// Check that it exists
		//
		try {
			database = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(id);
		} catch (Exception e) {
			return Response.status(404).build();
		}
		if (database == null){
			return Response.status(404).build();
		}
		
		//
		// Get the name of the database in PostgreSQL
		//
		String databaseName = database.getDbConsumedName();

		//
		// Check permission
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_MODIFY(database.getLogicalDatabaseId()))){
			
			//
			// If not, create an audit record for the attempt, and return 403
			//
			DatabaseStructureAuditService.Factory.getInstance().createNotAuthRecord(String.format("DELETE structure/%s/odbc/%s Not permitted", id, role), database.getLogicalDatabaseId());
			return Response.status(403).build();			
		}
		
		//
		// Drop role
		//
		StructureODBCService.Factory.getInstance().removeOdbcUserFromDatabase(role, database, databaseName);
		
		//
		// Create an audit record for the change
		// 
		DatabaseStructureAuditService.Factory.getInstance().removeODBCRole(database.getLogicalDatabaseId(), role);
		
		return Response.ok().build();
	}

}
