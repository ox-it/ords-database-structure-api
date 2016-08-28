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

package uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate;

import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.CachedRowSet;
import javax.ws.rs.NotFoundException;

import org.apache.shiro.SecurityUtils;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;

import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase.EntityType;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureRoleService;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;
import uk.ac.ox.it.ords.api.database.structure.model.SchemaDesignerTable;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissionSets;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.permissions.Permissions;
import uk.ac.ox.it.ords.security.services.PermissionsService;

public class DatabaseStructureServiceImpl extends StructureServiceImpl
		implements
			DatabaseStructureService {
	
	public void init() throws Exception {
		PermissionsService service = PermissionsService.Factory.getInstance();
		//
		// Anyone with the "User" role can contribute to projects
		//
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForUser()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("user");
			permissionObject.setPermission(permission);
			service.createPermission(permissionObject);
		}
		
		//
		// Anyone with the "LocalUser" role can create new trial projects
		//
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForLocalUser()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("localuser");
			permissionObject.setPermission(permission);
			service.createPermission(permissionObject);
		}
		
		//
		// Anyone with the "Administrator" role can create new full
		// projects and upgrade projects to full, and update any
		// user projects
		//
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForSysadmin()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("administrator");
			permissionObject.setPermission(permission);
			service.createPermission(permissionObject);
		}

		//
		// "Anonymous" can View public projects
		//
		for (String permission : DatabaseStructurePermissionSets.getPermissionsForAnonymous()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("anonymous");
			permissionObject.setPermission(permission);
			service.createPermission(permissionObject);
		}
	}
	

	@SuppressWarnings("unchecked")
	@Override
	public List<OrdsPhysicalDatabase> getDatabaseList() throws Exception {
		
		List<OrdsPhysicalDatabase> databases = null;
		ArrayList<OrdsPhysicalDatabase> visibleDatabases = new ArrayList<OrdsPhysicalDatabase>();
		
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			databases = session
					.createCriteria(OrdsPhysicalDatabase.class)
					.list();
			transaction.commit();
		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		}
		finally {
			session.close();
		}

		for (OrdsPhysicalDatabase database : databases){
			if (SecurityUtils.getSubject().isPermitted(Permissions.DATABASE_VIEW(database.getLogicalDatabaseId()))){
				visibleDatabases.add(database);
			}
		}
		
		return visibleDatabases;
	}	

	@Override
	public OrdsPhysicalDatabase getDatabaseMetaData(int dbId)
			throws Exception {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			@SuppressWarnings("unchecked")
			List<OrdsPhysicalDatabase> dbs = session
					.createCriteria(OrdsPhysicalDatabase.class)
					.add(Restrictions.eq("physicalDatabaseId", dbId))
					.list();
			transaction.commit();
			if (dbs.size() != 1) {
				throw new NotFoundException("Cannot find physical database id "
						+ dbId);
			}
			return dbs.get(0);
		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		}
		finally {
			session.close();
		}

	}

	@Override
	public TableList getDatabaseTableList(int dbId, boolean staging) throws Exception {

		OrdsPhysicalDatabase database = this.getDatabaseMetaData(dbId);
		if ( database == null ) {
			throw new NotFoundException();
		}
		String databaseName =  database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(database.getDbConsumedName());
		}
		String server = database.getDatabaseServer();

		String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name";
		// getting a single scalar value so hibernate returns a list of strings
		CachedRowSet results = this.runJDBCQuery(query, null, server, databaseName);
		//List results = this
		//		.runSQLQuery(query, databaseName, userName, password);
		TableList tables = new TableList();

		int counter = 0;
		int multiplier; // Convenience variables for displaying the tables
		// in slightly less cramped positions on the grid
		if (results.size() > 50) {
			multiplier = 1;
		} else if (results.size() > 25) {
			multiplier = 50;
		} else {
			multiplier = 120;
		}

		while( results.next() ) {
			String tableName = results.getString("table_name");
			// get the schema designer table for this table
			SchemaDesignerTable sdt = this.getSchemaDesignerTable(
					database.getPhysicalDatabaseId(), tableName.toString());
			String comment = this.tableComment(databaseName, server, tableName);
			tables.addTable(tableName, comment);
			addTableMetadata(databaseName, server, tableName, tables);
			if (sdt == null) {
				tables.setXY(tableName, counter * multiplier, counter
						* multiplier);
			} else {
				tables.setXY(tableName, sdt.getX(), sdt.getY());
			}
			counter++;
		}
		return tables;
	}

	@Override
	public String createNewStagingDatabase(int dbId)
			throws Exception {
		OrdsPhysicalDatabase database = this.getDatabaseMetaData(dbId);
		String stagingName = this.calculateStagingName(database
				.getDbConsumedName());
		
		//
		// Try to delete staging database if it already exists
		//
		// If this fails, its most likely as someone else is accessing it, for
		// example using the schema editor. This is probably going to cause an issue with
		// concurrent editing of the schema.
		//
		if (this.checkDatabaseExists(stagingName, database.getDatabaseServer())) {
			try {
				this.deleteDatabase(dbId, true);
			} catch (Exception e) {
				log.warn("Failed to delete staging database; this is most likely due to concurrent editing");
				log.debug(e.getMessage());
				return stagingName;
			}
		}
		String clonedb = String.format(
				"ROLLBACK TRANSACTION; CREATE DATABASE %s WITH TEMPLATE %s OWNER = %s",
				quote_ident(stagingName),
				quote_ident(database.getDbConsumedName()),
				quote_ident(this.getORDSDatabaseUser()));
		
		this.runJDBCQuery(clonedb, null, database.getDatabaseServer(), null);

		return stagingName;
	}

	@Override
	public void mergeStagingToActual(int dbId)
			throws Exception {
		OrdsPhysicalDatabase database = this.getDatabaseMetaData(dbId);
		String databaseName = database.getDbConsumedName();
		String stagingName = this.calculateStagingName(databaseName);

		if (!this.checkDatabaseExists(database.getDbConsumedName(), database.getDatabaseServer())) {
			throw new NotFoundException("Original database does not exist");
		}
		String sql = "rollback transaction; drop database " + databaseName
				+ ";";
		this.runJDBCQuery(sql, null, database.getDatabaseServer(), null);

		sql = String.format("ALTER DATABASE %s RENAME TO %s", stagingName,
				databaseName);
		this.runJDBCQuery(sql, null, database.getDatabaseServer(), null);


	}
	
	

	@Override
	public OrdsPhysicalDatabase mergeInstanceToMain(OrdsPhysicalDatabase source, OrdsPhysicalDatabase target)
			throws Exception {
		
		// check for source database
		String sourceDatabaseName = source.getDbConsumedName();
		if ( !this.checkDatabaseExists(sourceDatabaseName, source.getDatabaseServer())) {
			throw new NotFoundException("Source database does not exist");
		}
		// check for target database
		String targetDatabaseName = target.getDbConsumedName();
		if ( !this.checkDatabaseExists(targetDatabaseName, target.getDatabaseServer())) {
			throw new NotFoundException("Target database does not exist");
		}
		
		String sql = "rollback transaction; drop database " + targetDatabaseName
				+ ";";
		this.runJDBCQuery(sql, null, source.getDatabaseServer(), null);

		sql = String.format("ALTER DATABASE %s RENAME TO %s", 
				quote_ident(sourceDatabaseName),
				quote_ident(targetDatabaseName));
		this.runJDBCQuery(sql, null, source.getDatabaseServer(), null);

		
		// now we need to find and remove the row from physical database
		this.removeModelObject(source);

		return target;
	}


	@Override
	public void deleteDatabase(int dbId, boolean staging)
			throws Exception {
		OrdsPhysicalDatabase database = this.getDatabaseMetaData(dbId);
		String databaseName;
		if (!staging) {
			databaseName = database.getDbConsumedName();
			this.removeModelObject(database);
		} else {
			databaseName = this.calculateStagingName(database.getDbConsumedName());
		}
		String statement = this.getTerminateStatement(databaseName, database.getDatabaseServer());
		this.runJDBCQuery(statement, null, database.getDatabaseServer(), databaseName);
		statement = "rollback transaction; drop database " + databaseName + ";";
		this.runJDBCQuery(statement, null, database.getDatabaseServer(), null);

	}
	
	@Override
	public OrdsPhysicalDatabase createNewDatabaseFromExisting (int origDbId, DatabaseRequest dto ) throws Exception {

		OrdsPhysicalDatabase templateDb = this.getDatabaseMetaData(origDbId);
				
		// consumed name is on original
		String templateName = templateDb.getDbConsumedName();
		
		// create the new record
		OrdsPhysicalDatabase newDb = new OrdsPhysicalDatabase();
		newDb.setLogicalDatabaseId(templateDb.getLogicalDatabaseId());
		newDb.setDatabaseServer(templateDb.getDatabaseServer());
		newDb.setImportProgress(OrdsPhysicalDatabase.ImportType.FINISHED);
		EntityType type;
		if ( dto.getInstance().equalsIgnoreCase("MILESTONE")) {
			type = EntityType.MILESTONE;
		}
		else {
			type = EntityType.TEST;
		}
		newDb.setEntityType(type);
		newDb.setFileName("none");
		newDb.setFullPathToDirectory(System.getProperty("java.io.tmpdir")
				+ "/databases");
		newDb.setDatabaseType("RAW");
		this.saveModelObject(newDb);
		
		String newDatabaseName = newDb.getDbConsumedName();
				
		//
		// If this clone already exists, drop it.
		//
		if (this.checkDatabaseExists(newDatabaseName, newDb.getDatabaseServer())) {
			String statement = this.getTerminateStatement(newDatabaseName, newDb.getDatabaseServer());
			this.runJDBCQuery(statement, null, newDb.getDatabaseServer(), newDatabaseName);
			statement = "rollback transaction; drop database " + newDatabaseName + ";";
			this.runJDBCQuery(statement, null, newDb.getDatabaseServer(), null);

		}
		
		//
		// Create clone
		//
		String clonedb = String.format(
				"ROLLBACK TRANSACTION; CREATE DATABASE %s WITH TEMPLATE %s OWNER %s",
				quote_ident(newDatabaseName),
				quote_ident(templateName),
				quote_ident(this.getORDSDatabaseUser()));
		this.runJDBCQuery(clonedb, null, newDb.getDatabaseServer(), null);

		DatabaseStructureRoleService.Factory.getInstance().createInitialPermissions(newDb.getLogicalDatabaseId());
		return newDb;
	
	}

	@Override
	public OrdsPhysicalDatabase createNewDatabase(DatabaseRequest databaseDTO) throws Exception {
		log.debug("createEmptyDatabase");
		
		OrdsPhysicalDatabase db = new OrdsPhysicalDatabase();
		db.setLogicalDatabaseId(databaseDTO.getGroupId());
		db.setDatabaseServer(databaseDTO.getDatabaseServer());
		db.setImportProgress(OrdsPhysicalDatabase.ImportType.FINISHED);
		db.setEntityType(EntityType.MAIN);
		db.setFileName("none");
		db.setFullPathToDirectory(System.getProperty("java.io.tmpdir")
				+ "/databases");
		db.setDatabaseType("RAW");

		this.saveModelObject(db);

		if (db.getPhysicalDatabaseId() == 0) {
			log.error("Cannot get physical db id");
			return null;
		}
		
		String dbName = db.getDbConsumedName();
		String statement = String.format(
				"rollback transaction;create database %s owner %s;",
				quote_ident(dbName),
				quote_ident(this.getORDSDatabaseUser()));
				
		this.runJDBCQuery(statement, null, databaseDTO.getDatabaseServer(), null);
		
		String createSequence = "CREATE SEQUENCE ords_constraint_seq";
		String server = db.getDatabaseServer();
		this.runJDBCQuery(createSequence, null, server, dbName);
		
		DatabaseStructureRoleService.Factory.getInstance().createInitialPermissions(db.getLogicalDatabaseId());

		return db;
		
	}

	private SchemaDesignerTable getSchemaDesignerTable(int databaseId,
			String tableName) {
		log.debug("getTable:" + databaseId + "," + tableName);
		Transaction tx = null;
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			tx = session.beginTransaction();
			SQLQuery q = session
					.createSQLQuery(
							"SELECT * FROM schemadesignertable WHERE databaseid=:databaseid AND tablename=:tablename")
					.addEntity(SchemaDesignerTable.class);
			q.setParameter("databaseid", databaseId);
			q.setParameter("tablename", tableName);
			log.debug(q.toString());
			@SuppressWarnings("unchecked")
			List<SchemaDesignerTable> result = q.list();
			tx.commit();
			if (result.size() > 0) {
				return result.get(0);
			}
			return null;
		} catch (HibernateException e) {
			log.error("Run time exception", e);
			if (tx != null && tx.isActive()) {
				try {
					tx.rollback();
				} catch (HibernateException e1) {
				}
				throw e;
			}

			return null;
		} finally {
			session.close();
		}

	}

	@Override
	public boolean checkDatabaseExists(int dbId, boolean staging) throws Exception {

		OrdsPhysicalDatabase physicalDatabase = this.getDatabaseMetaData(dbId);
		String databaseName = physicalDatabase.getDbConsumedName();
		String server = physicalDatabase.getDatabaseServer();
		if (staging){
			databaseName = this.calculateStagingName(databaseName);
		}
		return checkDatabaseExists(databaseName, server);
	}
}