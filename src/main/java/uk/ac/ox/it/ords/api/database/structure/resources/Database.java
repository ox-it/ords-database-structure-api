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
import java.util.Locale;

import javax.annotation.PostConstruct;
import javax.ws.rs.Consumes;
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

import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.CommentRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.ConstraintRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.IndexRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.Language;
import uk.ac.ox.it.ords.api.database.structure.dto.PositionRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.TableRenameRequest;
import uk.ac.ox.it.ords.api.database.structure.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissions;
import uk.ac.ox.it.ords.api.database.structure.resources.AbstractResource.BooleanCheck;
import uk.ac.ox.it.ords.api.database.structure.services.ColumnStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.CommentService;
import uk.ac.ox.it.ords.api.database.structure.services.ConstraintService;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.IndexService;
import uk.ac.ox.it.ords.api.database.structure.services.MessageEntity;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;
import uk.ac.ox.it.ords.api.database.structure.services.TableStructureService;

@Path("/")
public class Database extends AbstractResource{
	
	@PostConstruct
	public void init() throws Exception {
		databaseServiceInstance().init();
	}

	
	/********************************************************
	 * Database Resources
	 ********************************************************/


	@GET
	@Produces( MediaType.APPLICATION_JSON )
	public Response getDatabases ( ) {
		if (!SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_VIEW_PUBLIC)) {
			return forbidden();
		}
		List<OrdsPhysicalDatabase> databaseList;
		try {
			databaseList = databaseServiceInstance().getDatabaseList();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
		return Response.ok(databaseList).build();
	}
	

	@GET
	@Path("{id}/{instance}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatabaseMetadata ( 	@PathParam("id") int dbId,
											@PathParam("instance") String instance) {
		TableList tableList;
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_VIEW(dbId))) {
			return forbidden();
		}
		try {
			tableList =  databaseServiceInstance().getDatabaseTableList(dbId, instance, false);
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
		return Response.ok(tableList).build();
	}

	
	@POST
	@Path("{id}/{instance}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response createDatabase ( @PathParam("id") int id,
										@PathParam("instance") String instance,
										DatabaseRequest databaseDTO){
		
		OrdsPhysicalDatabase newDatabase;
		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_CREATE)) {
			return forbidden();
		}
		try {
			newDatabase =  databaseServiceInstance().createNewDatabase(id, databaseDTO, instance);
		    //UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    //builder.path(Integer.toString(newDatabase.getPhysicalDatabaseId()));
		    return Response.status(Response.Status.CREATED).entity(newDatabase).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	@PUT
	@Path("{id}/{instance}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response mergeDatabaseToMain ( 
			@PathParam("id") int id,
			@PathParam("instance") String instance ) {
		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_MODIFY(id))) {
			return forbidden();
		}
		try {
			if ( instance.equalsIgnoreCase("MAIN")) {
				throw new BadParameterException("Cannot replace MAIN with itself!");
			}
			OrdsPhysicalDatabase merged = databaseServiceInstance().mergeInstanceToMain(id, instance);
			return Response.ok().entity(merged).build();
		}
		catch(Exception e ) {
			return this.handleException(e);
		}
		
	}
	
	
	@DELETE
	@Path("{id}/{instance}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response dropDatabase (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance ) {		
		
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_DELETE(dbId))) {
			return forbidden();		
		}
		try {
			databaseServiceInstance().deleteDatabase(dbId, instance, false);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	

	

// staging version resource	
	@GET
	@Path("{id}/{instance}/staging")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getStagingDatabaseMetadata ( 	@PathParam("id") int dbId,
											@PathParam("instance") String instance) {
		TableList tableList;
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_VIEW(dbId))) {
			return forbidden();
		}
		try {
			tableList =  databaseServiceInstance().getDatabaseTableList(dbId, instance, true);
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
		return Response.ok(tableList).build();
	}
	
	@POST
	@Path("{id}/{instance}/staging")
	@Produces( MediaType.APPLICATION_JSON )
	public Response createStagingDatabase (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@Context UriInfo uriInfo) {
		
		if (!canModifyDatabase(dbId)) {
			return forbidden();		
		}
		try {
			String databaseName = databaseServiceInstance().createNewStagingDatabase(dbId, instance);
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(databaseName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@PUT
	@Path("{id}/{instance}/staging")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response updateDatabaseMetadata (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance ) {
		
		if (!canModifyDatabase(dbId)) {
			return forbidden();		
		}
		try {
			databaseServiceInstance().mergeStagingToActual(dbId, instance);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	
	@DELETE
	@Path("{id}/{instance}/staging")
	@Produces( MediaType.APPLICATION_JSON )
	public Response dropStaginDatabase (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance ) {
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_DELETE(dbId))) {
			return forbidden();		
		}
		try {
			databaseServiceInstance().deleteDatabase(dbId, instance, true);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
		
	}
	
	
	
	/********************************************************
	 * Schema Designer Table Position
	 * TODO: This needs it's own microservice really
	 *******************************************************/
	@PUT
	@Path("{id}/{instance}/positions")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response saveTablePositions (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			PositionRequest request) {
		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_MODIFY(dbId))) {
			return forbidden();
		}
		try {
			this.tableServiceInstance().setTablePositions(dbId, instance, request);
			return Response.ok().build();
		}
		catch( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	

	/********************************************************
	 * Table Resources
	 ********************************************************/
	
	
	@GET
	@Path("{id}/{instance}/table/{tablename}/{staging}")
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
			TableList tableList = tableServiceInstance().getTableMetadata(
					dbId, instance, tableName, staging.getValue());
			return Response.ok(tableList).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@POST
	@Path("{id}/{instance}/table/{tablename}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
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
			tableServiceInstance().createNewTable(dbId, instance, tableName, staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(tableName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}		
	}
	
	
	@PUT
	@Path("{id}/{instance}/table/{tablename}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
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
			tableServiceInstance().renameTable(dbId, instance, tableName, request.getNewname(), staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	
	@DELETE
	@Path("{id}/{instance}/table/{tablename}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response deleteTable ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging ) {
		
		if(!canModifyDatabase(dbId)){
			return forbidden();
		}
		try {
			tableServiceInstance().deleteTable(dbId, instance, tableName, staging.getValue() );
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}	
	
	

	/********************************************************
	 * Column Resources
	 ********************************************************/

	@GET
	@Path("{id}/{instance}/table/{tablename}/column/{colname}/{staging}")
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
			ColumnRequest metadata = columnServiceInstance().getColumnMetadata(
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
			return this.handleException(e);
		}
	}
	
	@POST
	@Path("{id}/{instance}/table/{tablename}/column/{colname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response createColumn (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("colname") String columnName,
			@PathParam("staging") BooleanCheck staging,
			ColumnRequest newColumn,
			@Context UriInfo uriInfo ) {

		if (!canModifyDatabase(dbId)) {
			//TODO add audit service
			return forbidden();
		}		
		try {
			columnServiceInstance().createColumn(dbId, instance, tableName, columnName, newColumn, staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(tableName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@PUT
	@Path("{id}/{instance}/table/{tablename}/column/{colname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
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
			columnServiceInstance().updateColumn(dbId, instance, tableName, columnName, newColumn, staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@DELETE
	@Path("{id}/{instance}/table/{tablename}/column/{colname}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
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
			columnServiceInstance().deleteColumn(dbId, instance, tableName, columnName, staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}		
	}
	
	
	/********************************************************
	 * Comment Resources
	 ********************************************************/

	@GET
	@Path("{id}/{instance}/table/{tablename}/comment/{staging}")
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
					commentServiceInstance().getTableComment(dbId, instance, tableName, staging.getValue()));
			return Response.ok(comment).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@POST
	@PUT
	@Path("{id}/{instance}/table/{tablename}/comment/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response setTableComment (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging,
			CommentRequest newComment,
			@Context UriInfo uriInfo ) {
		
		if ( !canModifyDatabase( dbId )) {
			return forbidden();
		}
		try {
			commentServiceInstance().setTableComment(dbId, instance, tableName, newComment.getComment(), staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(tableName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@GET
	@Path("{id}/{instance}/table/{tablename}/column/{columnName}/comment/{staging}")
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
					commentServiceInstance().getColumnComment(dbId, instance, tableName, columnName, staging.getValue()));
			return Response.ok(comment).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@POST
	@PUT
	@Path("{id}/{instance}/table/{tablename}/column/{columnName}/comment/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response setColumnComment (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("columnName") String columnName,
			@PathParam("staging") BooleanCheck staging, 
			CommentRequest newComment,
			@Context UriInfo uriInfo ) {
		
		if ( !canModifyDatabase ( dbId ) ) {
			return forbidden();
		}
		try {
			commentServiceInstance().setColumnComment(
					dbId,
					instance,
					tableName,
					columnName,
					newComment.getComment(), 
					staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path("created");
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}	

	
	/********************************************************
	 * Constraint Resources
	 ********************************************************/

	@GET
	@Path("{id}/{instance}/table/{tablename}/constraint/{conname}/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTableConstraint ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("conname") String constraintName,
			@PathParam("staging") BooleanCheck staging ) {
		
		if (!canViewDatabase( dbId) ) {
			return forbidden();
		}
		try {
			MessageEntity e = constraintServiceInstance().getConstraint(
					dbId, 
					instance, 
					tableName, 
					constraintName,
					staging.getValue()
					);
			return Response.ok(e).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@POST
	@Path("{id}/{instance}/table/{tablename}/constraint/{conname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response createTableConstraint ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("conname") String constraintName,
			@PathParam("staging") BooleanCheck staging,
			ConstraintRequest constraint,
			@Context UriInfo uriInfo  ) {
		
		if (!canModifyDatabase( dbId ) ) {
			return forbidden();
		}
		try {
			constraintServiceInstance().createConstraint(dbId, instance, tableName, constraintName, constraint,
					staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(constraintName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}	
	}
	
	
	@PUT
	@Path("{id}/{instance}/table/{tablename}/constraint/{conname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response updateTableConstraint ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("conname") String constraintName,
			@PathParam("staging") BooleanCheck staging,
			ConstraintRequest constraint ) {
		if (!canModifyDatabase( dbId ) ) {
			return forbidden();
		}
		try {
			constraintServiceInstance().updateConstraint(dbId, instance, tableName,constraintName, constraint,
					staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}	
	}
	
	
	@DELETE
	@Path("/constraint/database/{id}/{instance}/table/{tablename}/constraint/{conname}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response deleteTableConstraint ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("conname") String constraintName,
			@PathParam("staging") BooleanCheck staging ) {
		if (!canModifyDatabase( dbId ) ) {
			return forbidden();
		}
		try {
			constraintServiceInstance().deleteConstraint(dbId, instance, tableName, constraintName,
					staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}		
	}
	
	
	
	/********************************************************
	 * Index Resources
	 ********************************************************/
	
	@GET
	@Path("{id}/{instance}/table/{tablename}/index/{indexname}/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getNamedIndex (  
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			@PathParam("staging") BooleanCheck staging ) {
		
		if (!canViewDatabase(dbId)){
			return forbidden();
		}
		try {
			MessageEntity index = indexServiceInstance().getIndex(dbId, instance, tableName, indexName,
					staging.getValue());
			return Response.ok(index).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@POST
	@Path("{id}/{instance}/table/{tablename}/index/{indexname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response createIndex (  
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			@PathParam("staging") BooleanCheck staging,
			IndexRequest newIndex,
			@Context UriInfo uriInfo ) {
		
		if(!canModifyDatabase(dbId)) {
			return forbidden();
		}
		try {
			indexServiceInstance().createIndex(dbId, instance, tableName, indexName, newIndex,
					staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(indexName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	@PUT
	@Path("{id}/{instance}/table/{tablename}/index/{indexname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response updateIndex (  
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			@PathParam("staging") BooleanCheck staging,
			IndexRequest newIndex ) {
		if(!canModifyDatabase(dbId)){
			return forbidden();
		}
		try {
			indexServiceInstance().updateIndex(dbId, instance, tableName, indexName, newIndex,
					staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@DELETE
	@Path("{id}/{instance}/table/{tablename}/index/{indexname}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response deleteIndex (  
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			@PathParam("staging") BooleanCheck staging) {
		
		if(!canModifyDatabase(dbId)){
			forbidden();
		}
		try {
			indexServiceInstance().deleteIndex(dbId, instance, tableName, indexName,
					staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
}
