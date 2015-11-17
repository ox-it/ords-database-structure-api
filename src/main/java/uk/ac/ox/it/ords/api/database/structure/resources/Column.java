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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.structure.metadata.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissions;
import uk.ac.ox.it.ords.api.database.structure.services.ColumnStructureService;

@Path("/database/{id}/{instance}/table/{tablename}/column/{colname}/{staging}")
public class Column extends AbstractResource {
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getColumnMetadata (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("colname") String columnName,
			@PathParam("staging") BooleanCheck staging) {
		if (!SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_VIEW(dbId))) {
			//TODO add audit service
			return Response.status(403).build();
		}
		try {
			ColumnRequest metadata = serviceInstance().getColumnMetadata(
					dbId, 
					instance, 
					tableName, 
					columnName,
					staging.getValue());
			if (metadata == null ) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}
			return Response.ok(metadata).build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createColumn (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("colname") String columnName,
			@PathParam("staging") BooleanCheck staging,
			ColumnRequest newColumn) {

		if (!canModifyDatabase(dbId)) {
			//TODO add audit service
			return forbidden();
		}		
		try {
			serviceInstance().createColumn(dbId, instance, tableName, columnName, newColumn, staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();			
		}
	}
	
	
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateColumn (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("colname") String columnName,
			@PathParam("staging") BooleanCheck staging,
			ColumnRequest newColumn) {

		if (!canModifyDatabase(dbId)) {
			//TODO add audit service
			return forbidden();
		}		try {
			serviceInstance().updateColumn(dbId, instance, tableName, columnName, newColumn, staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();			
		}
	}
	
	
	@DELETE
	public Response deleteColumn (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("colname") String columnName,
			@PathParam("staging") BooleanCheck staging ) {

		if (!canModifyDatabase(dbId)) {
			//TODO add audit service
			return forbidden();
		}
		try {
			serviceInstance().deleteColumn(dbId, instance, tableName, columnName, staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();			
		}		
	}

	private ColumnStructureService serviceInstance() {
		return ColumnStructureService.Factory.getInstance();
	}

}
