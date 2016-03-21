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

@Path("/")
public class Odbc {
	
	//
	// We use this to generate random passcodes
	//
	private SecureRandom random = new SecureRandom();
	
	@POST
	@Path("{id}/{instance}/odbc/")
	@Produces( MediaType.APPLICATION_JSON )
	public Response addOdbcRoleForCurrentUser(
			@PathParam("id") int id,
			@PathParam("instance") String instance
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
		
		try {
			database = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(id, instance);
		} catch (Exception e) {
			return Response.status(404).build();
		}
		
		if (database == null){
			return Response.status(404).build();
		}
		
		String databaseName = calculateInstanceName(database, instance);

		//
		// Generate a random password. We return this from the service, but never store it.
		//
		String password = new BigInteger(130, random).toString(32);
		
		//
		// Check permissions and create the appropriate role
		//
		if (SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_VIEW(id))){
			OdbcService.Factory.getInstance().addReadOnlyOdbcUserToDatabase(OdbcService.Factory.getInstance().getODBCUserName(databaseName), password, database, databaseName);
		} else {
			
			//
			// User has no permissions for this database so cannot have an ODBC role
			//
			return Response.status(403).build();
		}
		
		if (SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_MODIFY(id))){
			OdbcService.Factory.getInstance().addOdbcUserToDatabase(OdbcService.Factory.getInstance().getODBCUserName(databaseName), password, database, databaseName);	
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
	
	@DELETE
	@Path("{id}/{instance}/odbc/{role}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response removeOdbcRole(
			@PathParam("id") int id,
			@PathParam("instance") String instance,
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
		
		try {
			database = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(id, instance);
		} catch (Exception e) {
			return Response.status(404).build();
		}
		
		if (database == null){
			return Response.status(404).build();
		}
		
		String databaseName = calculateInstanceName(database, instance);

		//
		// Check permission
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_MODIFY(id))){
			return Response.status(403).build();			
		}
		
		//
		// TODO check role exists first? Or just let the service figure that one out?
		//
		OdbcService.Factory.getInstance().removeOdbcUserFromDatabase(role, database, databaseName);
		
		return Response.ok().build();
	}
	
	private String calculateInstanceName ( OrdsPhysicalDatabase db, String instance ){
		return 	(instance + "_" + db.getPhysicalDatabaseId() + "_" + db.getLogicalDatabaseId()).toLowerCase();
	}

}
