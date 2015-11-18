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

import java.util.List;

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

import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissions;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;

@Path("/database")
public class Database extends AbstractResource{
	

	@GET
	@Produces( MediaType.APPLICATION_JSON )
	public Response getDatabases ( ) {
		List<OrdsPhysicalDatabase> databaseList;
		try {
			databaseList = serviceInstance().getDatabaseList();
		}
		catch ( Exception e ) {
			// TODO: Handle exception properly and give a meaningful message to the user.
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
		return Response.ok(databaseList).build();
	}
	
	
	@POST
	@Path("{groupId}/{instance}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response createDatabase ( @PathParam("groupdId") int groupId,
										@PathParam("instance") String instance) {
		
		OrdsPhysicalDatabase newDatabase;
		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_CREATE)) {
			return forbidden();
		}
		try {
			newDatabase =  serviceInstance().createNewDatabase(groupId, instance);
		}
		catch ( Exception e ) {
			// TODO: proper exception handling!
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
		return Response.ok(newDatabase).build();
	}
	

	
	@GET
	@Path("{id}/{instance}/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatabaseMetadata ( 	@PathParam("id") int dbId,
											@PathParam("instance") String instance,
											@PathParam("staging") BooleanCheck staging) {
		TableList tableList;
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_VIEW(dbId))) {
			return forbidden();
		}
		try {
			tableList =  serviceInstance().getDatabaseTableList(dbId, instance, staging.getValue());
		}
		catch ( Exception e ) {
			// TODO:
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
		return Response.ok(tableList).build();
	}
	
	@POST
	@Path("{id}/{instance}/staging")
	@Produces(MediaType.APPLICATION_JSON)
	public Response createStagingDatabase (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance ) {
		
		if (!canModifyDatabase(dbId)) {
			return forbidden();		
		}
		try {
			String dbName = serviceInstance().createNewStagingDatabase(dbId, instance);
			return Response.ok(dbName).build();
		}
		catch ( Exception e ) {
			// TODO:
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	
	@PUT
	@Path("{id}/{instance}/staging")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateDatabaseMetadata (
			OrdsPhysicalDatabase update,
			@PathParam("id") int dbId,
			@PathParam("instance") String instance ) {
		
		if (!canModifyDatabase(dbId)) {
			return forbidden();		
		}
		try {
			serviceInstance().updateStagingDatabase(dbId, update);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	
	@DELETE
	@Path("{id}/{instance}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response dropDatabase (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance ) {		
		
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_DELETE(dbId))) {
			return forbidden();		
		}
		try {
			serviceInstance().deleteDatabase(dbId, instance, false);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	
	@DELETE
	@Path("{id}/{instance}/staging")
	@Produces(MediaType.APPLICATION_JSON)
	public Response dropStaginDatabase (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance ) {
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_DELETE(dbId))) {
			return forbidden();		
		}
		try {
			serviceInstance().deleteDatabase(dbId, instance, true);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
		
	}
	
	@POST
	@Path("{id}/{instance}/data")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response handleFileUpload ( @PathParam("id") int dbId,
			@PathParam("instance") String instance,
			List<Attachment> attachments) {
		if (!canModifyDatabase(dbId)) {
			return forbidden();		
		}
		return Response.ok().build();
	}
	
	
	private DatabaseStructureService serviceInstance ( ) {
		return DatabaseStructureService.Factory.getInstance();
	}
	
}
