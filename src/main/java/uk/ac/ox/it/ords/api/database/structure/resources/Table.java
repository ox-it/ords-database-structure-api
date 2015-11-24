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

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import uk.ac.ox.it.ords.api.database.structure.services.TableList;
import uk.ac.ox.it.ords.api.database.structure.services.TableStructureService;
import uk.ac.ox.it.ords.api.database.structure.metadata.TableRenameRequest;

@Path("table/database/{id}/{instance}/table/{tablename}/{staging}")
public class Table extends AbstractResource {
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTableMetadata ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging ) {
		
		if (!canViewDatabase(dbId)){
			return forbidden();
		}
		try {
			TableList tableList = serviceInstance().getTableMetadata(
					dbId, instance, tableName, staging.getValue());
			return Response.ok(tableList).build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	
	@POST
	public Response createNewTable ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging,
			@Context UriInfo uriInfo ) {
		
		if (!canModifyDatabase(dbId)){
			return forbidden();
		}
		try {
			serviceInstance().createNewTable(dbId, instance, tableName, staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(tableName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}		
	}
	
	
	@PUT
	public Response updateTableName ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging,
			TableRenameRequest request) {
		
		if(!canModifyDatabase(dbId)){
			return forbidden();
		}
		try {
			serviceInstance().renameTable(dbId, instance, tableName, request.getNewname(), staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();			
		}
	}
	
	
	
	@DELETE
	public Response deleteTable ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging ) {
		
		if(!canModifyDatabase(dbId)){
			return forbidden();
		}
		try {
			serviceInstance().deleteTable(dbId, instance, tableName, staging.getValue() );
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();			
		}
	}	
	
	
	private TableStructureService serviceInstance() {
		return TableStructureService.Factory.getInstance();
	}
}
