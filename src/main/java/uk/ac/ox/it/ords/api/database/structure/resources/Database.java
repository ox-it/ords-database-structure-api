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
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ResponseHeader;

import java.net.URI;
import java.util.List;

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
import uk.ac.ox.it.ords.api.database.structure.dto.PositionRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.TableRenameRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissions;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.MessageEntity;
import uk.ac.ox.it.ords.api.database.structure.services.OdbcService;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;

/**
 * The REST API for database structure
 * @Refactor split this into separate classes handling their concerns
 * @author scottw
 *
 */
@Api(value="Database Structure")
@Path("/")
public class Database extends AbstractResource{
	
	@PostConstruct
	public void init() throws Exception {
		databaseServiceInstance().init();
	}

	
	/********************************************************
	 * Database Resources
	 ********************************************************/

	@ApiOperation(
			value="Gets a list of databases", 
			notes="Returns the databases for the current user, or the public databases if not logged in", 
			response = uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase.class, 
			responseContainer = "List"
			)
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
	
	@ApiOperation(
			value="Gets the metadata for a specific database", 
			notes="Specifically this lists the tables in the database.", 
			response = uk.ac.ox.it.ords.api.database.structure.services.TableList.class
			)
	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatabaseMetadata ( 	@PathParam("id") int dbId) {
		TableList tableList;
		OrdsPhysicalDatabase physicalDatabase = null;
		
		//
		// Try and obtain the database
		//
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		//
		// Check we are allowed to view it
		//
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
			return forbidden();
		}
		
		try {
			tableList =  databaseServiceInstance().getDatabaseTableList(dbId, false);
		}
		
		catch ( Exception e ) {
			return this.handleException(e);
		}
		
		return Response.ok(tableList).build();
	}


	@ApiOperation(
			value="Creates a new empty database", 
			notes="", 
			response = uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase.class
			)
	@POST
	@Path("")
	@Produces(MediaType.APPLICATION_JSON)
	public Response createDatabase (DatabaseRequest databaseDTO){
		
		OrdsPhysicalDatabase newDatabase;
		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_CREATE)) {
			return forbidden();
		}
		try {
			newDatabase =  databaseServiceInstance().createNewDatabase(databaseDTO);
		    //UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    //builder.path(Integer.toString(newDatabase.getPhysicalDatabaseId()));
		    return Response.status(Response.Status.CREATED).entity(newDatabase).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	@ApiOperation(
			value="Creates a copy of the specified database as a new database", 
			notes="", 
			response = uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase.class
			)
	@POST
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response cloneDatabase ( @PathParam("id") int id,
										DatabaseRequest databaseDTO){
		
		OrdsPhysicalDatabase newDatabase;
		OrdsPhysicalDatabase templateDatabase;
		
		try {
			 templateDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(id);
		} catch (Exception e) {
			return Response.status(404).build();
		}
		if (templateDatabase == null){
			return Response.status(404).build();
		}

		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_CREATE)) {
			return forbidden();
		}
		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_VIEW(templateDatabase.getLogicalDatabaseId()))) {
			return forbidden();
		}
		
		try {
			newDatabase =  databaseServiceInstance().createNewDatabaseFromExisting(id, databaseDTO);
		    //UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    //builder.path(Integer.toString(newDatabase.getPhysicalDatabaseId()));
		    return Response.status(Response.Status.CREATED).entity(newDatabase).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	@ApiOperation(
			value="Merges another database into this one", 
			notes="Note the resource in the URL is the target and is overwritten by the source indicated with the getCloneFrom property", 
			response = uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase.class
			)
	@PUT
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response mergeDatabaseToMain ( 
			@PathParam("id") int id,
			DatabaseRequest databaseDTO
			) {
		
		OrdsPhysicalDatabase target = null;
		OrdsPhysicalDatabase source = null;
		
		//
		// Try and obtain the target database
		//
		try {
			target = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(id);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		//
		// Try and obtain the source database to merge into it
		//
		if (databaseDTO == null || databaseDTO.getCloneFrom() == null){
			return Response.status(400).build();
		}
		try {
			source = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData( databaseDTO.getCloneFrom());
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_MODIFY(target.getLogicalDatabaseId()))) {
			return forbidden();
		}
		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_MODIFY(source.getLogicalDatabaseId()))) {
			return forbidden();
		}
		try {
			OrdsPhysicalDatabase merged = databaseServiceInstance().mergeInstanceToMain(source, target);
			return Response.ok().entity(merged).build();
		}
		catch(Exception e ) {
			return this.handleException(e);
		}
		
	}
	
	@ApiOperation(
			value="Deletes the specified database", 
			notes=""
			)
	@DELETE
	@Path("{id}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response dropDatabase (
			@PathParam("id") int dbId) {	
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_DELETE(physicalDatabase.getLogicalDatabaseId()))) {
			return forbidden();		
		}
		try {
			OdbcService.Factory.getInstance().removeAllODBCRolesFromDatabase(physicalDatabase);
			databaseServiceInstance().deleteDatabase(dbId, false);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	

	

	@ApiOperation(
			value="Gets the metadata for a staging database", 
			notes="Specifically this lists the tables in the database.", 
			response = uk.ac.ox.it.ords.api.database.structure.services.TableList.class
			)
	@GET
	@Path("{id}/staging")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getStagingDatabaseMetadata ( 	
			@PathParam("id") int dbId
	) throws Exception {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		//
		// Check the staging version exists
		//
		if (!DatabaseStructureService.Factory.getInstance().checkDatabaseExists(dbId, true)){
			return Response.status(404).build();
		}		

		TableList tableList;
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
			return forbidden();
		}
		try {
			tableList =  databaseServiceInstance().getDatabaseTableList(dbId, true);
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
		return Response.ok(tableList).build();
	}
	
	@ApiOperation(
			value="Creates a new staging database for the specified database", 
			notes="" 
			)
	@ApiResponses(value = { 
			@ApiResponse(code = 201, message = "Staging database successfully created.",
					responseHeaders = @ResponseHeader(name = "Location", description = "The URI of the staging database", response = URI.class)
					),
		    @ApiResponse(code = 404, message = "Original database does not exist."),
		    @ApiResponse(code = 403, message = "Not authorized to create a staging database.")
	})
	@POST
	@Path("{id}/staging")
	@Produces( MediaType.APPLICATION_JSON )
	public Response createStagingDatabase (
			@PathParam("id") int dbId,
			@Context UriInfo uriInfo) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if (!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())) {
			return forbidden();		
		}
		try {
			String databaseName = databaseServiceInstance().createNewStagingDatabase(dbId);
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    //builder.path(databaseName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	@ApiOperation(
			value="Merges a staging database back into the original database it was derived from", 
			notes="" 
			)
	@ApiResponses(value = { 
			@ApiResponse(code = 200, message = "Staging database successfully merged."),
		    @ApiResponse(code = 404, message = "Original database does not exist."),
		    @ApiResponse(code = 403, message = "Not authorized to create a staging database.")
	})
	@PUT
	@Path("{id}/staging")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response mergeStagingToMain (
			@PathParam("id") int dbId
			) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if (!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())) {
			return forbidden();		
		}
		try {
			databaseServiceInstance().mergeStagingToActual(dbId);
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@ApiOperation(
			value="Deletes a staging database", 
			notes="" 
			)
	@ApiResponses(value = { 
			@ApiResponse(code = 200, message = "Staging database successfully deleted."),
		    @ApiResponse(code = 404, message = "Staging database does not exist."),
		    @ApiResponse(code = 403, message = "Not authorized to delete staging database.")
	})
	@DELETE
	@Path("{id}/staging")
	@Produces( MediaType.APPLICATION_JSON )
	public Response dropStaginDatabase (
			@PathParam("id") int dbId
	) throws Exception{
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		//
		// Check the staging version exists
		//
		if (!DatabaseStructureService.Factory.getInstance().checkDatabaseExists(dbId, true)){
			return Response.status(404).build();
		}
		
		if (!SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_DELETE(physicalDatabase.getLogicalDatabaseId()))) {
			return forbidden();		
		}
		try {
			databaseServiceInstance().deleteDatabase(dbId, true);
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
	@Path("{id}/positions")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response saveTablePositions (
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			PositionRequest request) {
		

		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if ( !SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
			return forbidden();
		}
		try {
			this.tableServiceInstance().setTablePositions(physicalDatabase, request);
			return Response.ok().build();
		}
		catch( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	

	/********************************************************
	 * Table Resources
	 ********************************************************/
	
	@ApiOperation(
			value="Gets metadata for a table", 
			notes="",
			response = uk.ac.ox.it.ords.api.database.structure.services.TableList.class
			)
	@ApiResponses(value = { 
			@ApiResponse(code = 200, message = "Table metadata retrieved."),
		    @ApiResponse(code = 404, message = "Table does not exist."),
		    @ApiResponse(code = 403, message = "Not authorized to view table.")
	})
	@GET
	@Path("{id}/table/{tablename}/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTableMetadata ( 
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if (!canViewDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		try {
			TableList tableList = tableServiceInstance().getTableMetadata(
					physicalDatabase, tableName, staging.getValue());
			return Response.ok(tableList).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	@ApiOperation(
			value="Create a table", 
			notes="",
			response = uk.ac.ox.it.ords.api.database.structure.services.TableList.class
			)
	@ApiResponses(value = { 
			@ApiResponse(code = 201, message = "Table successfully created.",
					responseHeaders = @ResponseHeader(name = "Location", description = "The URI of the table", response = URI.class)
					),
		    @ApiResponse(code = 404, message = "Database does not exist."),
		    @ApiResponse(code = 403, message = "Not authorized to create table.")
	})
	@POST
	@Path("{id}/table/{tablename}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response createNewTable ( 
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging,
			@Context UriInfo uriInfo ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if (!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		try {
			tableServiceInstance().createNewTable(physicalDatabase, tableName, staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(tableName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}		
	}
	
	
	@PUT
	@Path("{id}/table/{tablename}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response updateTableName ( 
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging,
			TableRenameRequest request) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		try {
			tableServiceInstance().renameTable(physicalDatabase, tableName, request.getNewname(), staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	
	@DELETE
	@Path("{id}/table/{tablename}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response deleteTable ( 
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			tableServiceInstance().deleteTable(physicalDatabase, tableName, staging.getValue() );
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
	@Path("{id}/table/{tablename}/column/{colname}/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getColumnMetadata (
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("colname") String columnName,
			@PathParam("staging") BooleanCheck staging) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if (!SecurityUtils.getSubject().isPermitted(DatabaseStructurePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
			//TODO add audit service
			return Response.status(403).build();
		}
		try {
			ColumnRequest metadata = columnServiceInstance().getColumnMetadata(
					physicalDatabase, 
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
	@Path("{id}/table/{tablename}/column/{colname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response createColumn (
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("colname") String columnName,
			@PathParam("staging") BooleanCheck staging,
			ColumnRequest newColumn,
			@Context UriInfo uriInfo ) {

		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		if (newColumn == null){
			return Response.status(400).build();	
		}
		
		try {
			columnServiceInstance().createColumn(physicalDatabase, tableName, columnName, newColumn, staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(tableName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@PUT
	@Path("{id}/table/{tablename}/column/{colname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response updateColumn (
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("colname") String columnName,
			@PathParam("staging") BooleanCheck staging,
			ColumnRequest newColumn) {

		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			columnServiceInstance().updateColumn(physicalDatabase, tableName, columnName, newColumn, staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@DELETE
	@Path("{id}/table/{tablename}/column/{colname}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response deleteColumn (
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("colname") String columnName,
			@PathParam("staging") BooleanCheck staging ) {

		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			columnServiceInstance().deleteColumn(physicalDatabase, tableName, columnName, staging.getValue());
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
	@Path("{id}/table/{tablename}/comment/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTableComment ( 
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canViewDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			CommentRequest comment = new CommentRequest();
			comment.setComment(
					commentServiceInstance().getTableComment(physicalDatabase, tableName, staging.getValue()));
			return Response.ok(comment).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@POST
	@PUT
	@Path("{id}/table/{tablename}/comment/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response setTableComment (
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("staging") BooleanCheck staging,
			CommentRequest newComment,
			@Context UriInfo uriInfo ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			commentServiceInstance().setTableComment(physicalDatabase, tableName, newComment.getComment(), staging.getValue());
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(tableName);
		    return Response.created(builder.build()).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@GET
	@Path("{id}/table/{tablename}/column/{columnName}/comment/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getColumnComment ( 
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("columnName") String columnName,
			@PathParam("staging") BooleanCheck staging ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canViewDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			CommentRequest comment = new CommentRequest();
			comment.setComment(
					commentServiceInstance().getColumnComment(physicalDatabase, tableName, columnName, staging.getValue()));
			return Response.ok(comment).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@POST
	@PUT
	@Path("{id}/table/{tablename}/column/{columnName}/comment/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response setColumnComment (
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("columnName") String columnName,
			@PathParam("staging") BooleanCheck staging, 
			CommentRequest newComment,
			@Context UriInfo uriInfo ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			commentServiceInstance().setColumnComment(
					physicalDatabase,
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
	@Path("{id}/table/{tablename}/constraint/{conname}/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTableConstraint ( 
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("conname") String constraintName,
			@PathParam("staging") BooleanCheck staging ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e) {
			return Response.status(404).build();
		}
		
		if(!canViewDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			MessageEntity e = constraintServiceInstance().getConstraint(
					physicalDatabase,
					tableName, 
					constraintName,
					staging.getValue()
					);
			return Response.ok(e).build();
		}
		catch ( Exception e ) {
			e.printStackTrace();
			return this.handleException(e);
		}
	}
	
	
	@POST
	@Path("{id}/table/{tablename}/constraint/{conname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response createTableConstraint ( 
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("conname") String constraintName,
			@PathParam("staging") BooleanCheck staging,
			ConstraintRequest constraint,
			@Context UriInfo uriInfo  ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			constraintServiceInstance().createConstraint(physicalDatabase, tableName, constraintName, constraint,
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
	@Path("{id}/table/{tablename}/constraint/{conname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response updateTableConstraint ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("conname") String constraintName,
			@PathParam("staging") BooleanCheck staging,
			ConstraintRequest constraint ) {
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			constraintServiceInstance().updateConstraint(physicalDatabase, tableName,constraintName, constraint,
					staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}	
	}
	
	
	@DELETE
	@Path("/{id}/table/{tablename}/constraint/{conname}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response deleteTableConstraint ( 
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("conname") String constraintName,
			@PathParam("staging") BooleanCheck staging ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			constraintServiceInstance().deleteConstraint(physicalDatabase, tableName, constraintName,
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
	@Path("{id}/table/{tablename}/index/{indexname}/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getNamedIndex (  
			@PathParam("id") int dbId,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			@PathParam("staging") BooleanCheck staging ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canViewDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			MessageEntity index = indexServiceInstance().getIndex(physicalDatabase, tableName, indexName,
					staging.getValue());
			return Response.ok(index).build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@POST
	@Path("{id}/table/{tablename}/index/{indexname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response createIndex (  
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			@PathParam("staging") BooleanCheck staging,
			IndexRequest newIndex,
			@Context UriInfo uriInfo ) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			indexServiceInstance().createIndex(physicalDatabase, tableName, indexName, newIndex,
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
	@Path("{id}/table/{tablename}/index/{indexname}/{staging}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces( MediaType.APPLICATION_JSON )
	public Response updateIndex (  
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			@PathParam("staging") BooleanCheck staging,
			IndexRequest newIndex ) {
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			indexServiceInstance().updateIndex(physicalDatabase, tableName, indexName, newIndex,
					staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
	
	
	@DELETE
	@Path("{id}/table/{tablename}/index/{indexname}/{staging}")
	@Produces( MediaType.APPLICATION_JSON )
	public Response deleteIndex (  
			@PathParam("id") int dbId,
			@PathParam("tablename") String tableName,
			@PathParam("indexname") String indexName,
			@PathParam("staging") BooleanCheck staging) {
		
		//
		// Try and obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = null;
		try {
			physicalDatabase = DatabaseStructureService.Factory.getInstance().getDatabaseMetaData(dbId);
		} catch (Exception e1) {
			return Response.status(404).build();
		}
		
		if(!canModifyDatabase(physicalDatabase.getLogicalDatabaseId())){
			return forbidden();
		}
		
		try {
			indexServiceInstance().deleteIndex(physicalDatabase, tableName, indexName,
					staging.getValue());
			return Response.ok().build();
		}
		catch ( Exception e ) {
			return this.handleException(e);
		}
	}
}
