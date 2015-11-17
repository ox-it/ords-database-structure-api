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

import uk.ac.ox.it.ords.api.database.structure.metadata.IndexRequest;
import uk.ac.ox.it.ords.api.database.structure.services.IndexService;

@Path("/database/{id}/{instance}/table/{tablename}/index/{indexname}")
public class Index extends AbstractResource {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getNamedIndex (  
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName ) {
		
		if (!canViewDatabase(dbId)){
			return forbidden();
		}
		try {
			IndexRequest index = serviceInstance().getIndex(dbId, instance, tableName, indexName);
			return Response.ok(index).build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createIndex (  
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			IndexRequest newIndex ) {
		
		if(!canModifyDatabase(dbId)) {
			return forbidden();
		}
		try {
			serviceInstance().createIndex(dbId, instance, tableName, newIndex);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateIndex (  
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			IndexRequest newIndex ) {
		if(!canModifyDatabase(dbId)){
			return forbidden();
		}
		try {
			serviceInstance().updateIndex(dbId, instance, tableName, indexName, newIndex);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	
	@DELETE
	public Response deleteIndex (  
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName) {
		
		if(!canModifyDatabase(dbId)){
			forbidden();
		}
		try {
			serviceInstance().deleteIndex(dbId, instance, tableName, indexName);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			//TODO
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}	
	
	private IndexService serviceInstance() {
		return IndexService.Factory.getInstance();
	}
}
