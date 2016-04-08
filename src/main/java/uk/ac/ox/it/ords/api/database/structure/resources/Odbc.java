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
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.OdbcService;

/**
 * API for requesting and revoking ODBC access
 */
@Path("/")
public class Odbc {
	
	//
	// We use this to generate random passcodes
	//
	private SecureRandom random = new SecureRandom();
	
	/**
	 * Request ODBC access to a database for the current subject. This will generate new credentials, which
	 * are returned to the requestor, but are not stored anywhere within ORDS.
	 * @param id
	 * @return an ODBCResponse containing all the connection details needed by a client to connect to the database by ODBC
	 * @throws Exception
	 */
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
			OdbcService.Factory.getInstance().addOdbcUserToDatabase(OdbcService.Factory.getInstance().getODBCUserName(databaseName), password, database, databaseName);	

		} else if (SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_VIEW(database.getLogicalDatabaseId()))){
			
			//
			// User has View rights, so create a read-only ODBC role
			//
			OdbcService.Factory.getInstance().addReadOnlyOdbcUserToDatabase(OdbcService.Factory.getInstance().getODBCUserName(databaseName), password, database, databaseName);

		} else {
			
			//
			// User has no permissions for this database so cannot have an ODBC role
			//
			return Response.status(403).build();
		}
		
		
		//
		// We return the generated password. We could alternatively email it to the user.
		//
		OdbcResponse response = new OdbcResponse();
		response.setServer(database.getDatabaseServer());
		response.setDatabase(databaseName);
		response.setPassword(password);
		response.setUsername(OdbcService.Factory.getInstance().getODBCUserName(databaseName));
		return Response.ok(response).build();
	}
	
	/**
	 * Revokes ODBC access on a database for all roles
	 * @param id the database id
	 * @return
	 * @throws Exception
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
		// Check permission - we want more than just "modify" permission for this
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_DELETE(database.getLogicalDatabaseId()))){
			return Response.status(403).build();			
		}
		
		//
		// Remove all roles
		//
		OdbcService.Factory.getInstance().removeAllODBCRolesFromDatabase(database);
		
		return Response.ok().build();
	}

	
	/**
	 * Revokes ODBC access on a database for a role
	 * @param id the database id
	 * @param role the name of the role to revoke
	 * @return
	 * @throws Exception
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
			return Response.status(403).build();			
		}
		
		//
		// TODO check role exists first? Or just let the service figure that one out?
		//
		OdbcService.Factory.getInstance().removeOdbcUserFromDatabase(role, database, databaseName);
		
		return Response.ok().build();
	}

}
