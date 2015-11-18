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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import uk.ac.ox.it.ords.api.database.structure.metadata.CommentRequest;
import uk.ac.ox.it.ords.api.database.structure.services.CommentService;

@Path("/database/{id}/{instance}/comment/{tablename}/{staging}")
public class Comment extends AbstractResource {
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTableComment ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging ) {
		
		if ( !canViewDatabase ( dbId ) ) {
			return forbidden();
		}
		try {
			CommentRequest comment = new CommentRequest();
			comment.setComment(
					serviceInstance().getTableComment(dbId, instance, tableName, staging.getValue()));
			return Response.ok(comment).build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	
	@POST
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response setTableComment (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging,
			CommentRequest newComment ) {
		
		if ( !canModifyDatabase( dbId )) {
			return forbidden();
		}
		try {
			serviceInstance().setTableComment(dbId, instance, tableName, newComment.getComment(), staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	
	@GET
	@Path("{columnName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getColumnComment ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("columnName") String columnName,
			@PathParam("staging") BooleanCheck staging ) {
		
		if ( !canViewDatabase( dbId )) {
			return forbidden();
		}
		try {
			CommentRequest comment = new CommentRequest();
			comment.setComment(
					serviceInstance().getColumnComment(dbId, instance, tableName, columnName, staging.getValue()));
			return Response.ok(comment).build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	
	@POST
	@PUT
	@Path("{columnName}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response setTableComment (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("columnName") String columnName,
			@PathParam("staging") BooleanCheck staging, 
			CommentRequest newComment ) {
		
		if ( !canModifyDatabase ( dbId ) ) {
			return forbidden();
		}
		try {
			serviceInstance().setColumnComment(
					dbId,
					instance,
					tableName,
					columnName,
					newComment.getComment(), 
					staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}	



	private CommentService serviceInstance( ) {
		return CommentService.Factory.getInstance();
	}
}
