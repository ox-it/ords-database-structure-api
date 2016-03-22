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
import uk.ac.ox.it.ords.api.database.structure.model.OrdsDB;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase.EntityType;
import uk.ac.ox.it.ords.api.database.structure.model.User;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureRoleService;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;
import uk.ac.ox.it.ords.api.database.structure.model.SchemaDesignerTable;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissionSets;
import uk.ac.ox.it.ords.security.model.Permission;
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
	

	@Override
	public List<OrdsPhysicalDatabase> getDatabaseList() throws Exception {
		String principalName = SecurityUtils.getSubject().getPrincipal()
				.toString();

		User u = this.getUserByPrincipal(principalName);

		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			@SuppressWarnings("unchecked")
			List<OrdsPhysicalDatabase> dbs = session
					.createCriteria(OrdsPhysicalDatabase.class)
					.add(Restrictions.eq("actorId", u.getUserId())).list();
			transaction.commit();
			return dbs;
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
	public OrdsPhysicalDatabase createNewDatabase(int id, DatabaseRequest databaseDTO,
			String instance) throws Exception {
		String username = this.getODBCUserName();
		String password = this.getODBCPassword();
		this.createOBDCUserRole(username, password);
		OrdsPhysicalDatabase db;
		if ( instance.equalsIgnoreCase("MAIN")) {
			db = this.createEmptyDatabase(id, databaseDTO, instance,
					username, password);
		}
		else {
			db = this.cloneDatabase(id, instance, username);
		}
		
		DatabaseStructureRoleService.Factory.getInstance()
					.createInitialPermissions(getLogicalDatabase(db.getLogicalDatabaseId()));
		return db;
	}
	
	

	@Override
	public OrdsPhysicalDatabase getDatabaseMetaData(int dbId, String instance)
			throws Exception {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			EntityType dbType = OrdsPhysicalDatabase.EntityType
					.valueOf(instance.toUpperCase());
			@SuppressWarnings("unchecked")
			List<OrdsPhysicalDatabase> dbs = session
					.createCriteria(OrdsPhysicalDatabase.class)
					.add(Restrictions.eq("physicalDatabaseId", dbId))
					.add(Restrictions.eq("entityType", dbType)).list();
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
	public TableList getDatabaseTableList(int dbId, String instance,
			boolean staging) throws Exception {
//		String userName = this.getODBCUserName();
//		String password = this.getODBCPassword();
		// we need to get the physical database this time because we need it's
		// id later
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		if ( database == null ) {
			throw new NotFoundException();
		}
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
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
	public String createNewStagingDatabase(int dbId, String instance)
			throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(
				dbId, instance);
		String stagingName = this.calculateStagingName(database
				.getDbConsumedName());
		if (this.checkDatabaseExists(stagingName)) {
			this.deleteDatabase(dbId, instance, true);
		}
		String clonedb = String.format(
				"ROLLBACK TRANSACTION; CREATE DATABASE %s WITH TEMPLATE %s OWNER = %s",
				quote_ident(stagingName),
				quote_ident(database.getDbConsumedName()),
				quote_ident(userName));
		this.runSQLStatementOnOrdsDB(clonedb);
		//String sequenceName = "ords_constraint_seq";
		//String createSequence = String.format("CREATE SEQUENCE %s",
		//quote_ident(sequenceName));
		//this.runSQLStatement(createSequence, stagingName, userName, password);
		//String createSequence = "CREATE SEQUENCE ords_constraint_seq";
		//String server = database.getDatabaseServer();
		//this.runJDBCQuery(createSequence, null, server, stagingName);
		
		
		return stagingName;
	}
	

	@Override
	public void updateStagingDatabase(int dbId, OrdsPhysicalDatabase update)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void mergeStagingToActual(int dbId, String instance)
			throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(
				dbId, instance);
		String databaseName = database.getDbConsumedName();
		String stagingName = this.calculateStagingName(databaseName);

		if (!this.checkDatabaseExists(database.getDbConsumedName())) {
			throw new NotFoundException("Original database does not exist");
		}
		String sql = "rollback transaction; drop database " + databaseName
				+ ";";
		this.runSQLStatementOnOrdsDB(sql);
		sql = String.format("ALTER DATABASE %s RENAME TO %s", stagingName,
				databaseName);
		this.runSQLStatementOnOrdsDB(sql);

	}
	
	

	@Override
	public OrdsPhysicalDatabase mergeInstanceToMain(int dbId, String instance)
			throws Exception {
		// check for original database
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, "MAIN");
		String databaseName = database.getDbConsumedName();
		String instanceDbName = this.calculateInstanceName(database, instance);
		
		if ( !this.checkDatabaseExists(databaseName)) {
			throw new NotFoundException("Original database does not exist");
		}
		String sql = "rollback transaction; drop database " + databaseName
				+ ";";
		this.runSQLStatementOnOrdsDB(sql);
		sql = String.format("ALTER DATABASE %s RENAME TO %s", instanceDbName,
				databaseName);
		this.runSQLStatementOnOrdsDB(sql);
		
		// now we need to find and remove the row from physical database
		
		// I think consumed name should be unique
		OrdsPhysicalDatabase dbRowToDelete = this.getDBOnCriteria("dbConsumedName", instanceDbName);
		this.removeModelObject(dbRowToDelete);

		return database;
	}


	@Override
	public void deleteDatabase(int dbId, String instance, boolean staging)
			throws Exception {
		// TODO Auto-generated method stub
		OrdsPhysicalDatabase database = getPhysicalDatabaseFromIDInstance(dbId,
				instance);
		String databaseName;
		if (!staging) {
			databaseName = database.getDbConsumedName();
			this.removeModelObject(database);
		} else {
			databaseName = this.calculateStagingName(database
					.getDbConsumedName());
		}
		String statement = this.getTerminateStatement(databaseName);
		this.runSQLQuery(statement, null, null, null);
		statement = "rollback transaction; drop database " + databaseName + ";";
		this.runSQLStatementOnOrdsDB(statement);
	}

	private void createOBDCUserRole(String username, String password)
			throws Exception {

		// check if role exists already
		String sql = String.format("SELECT 1 FROM pg_roles WHERE rolname='%s'",
				username);
		@SuppressWarnings("rawtypes")
		List r = this.runSQLQuery(sql, null, null, null);
		if (r.size() == 0) {
			// role doesn't exist
			String command = String
					.format("create role \"%s\" nosuperuser login createdb inherit nocreaterole password '%s' valid until '2045-01-01'",
							username, password);
			this.runSQLStatementOnOrdsDB(command);
		}
	}
	
	
	private OrdsPhysicalDatabase cloneDatabase (int origDbId, String newInstance, String userName ) throws Exception {
		OrdsPhysicalDatabase origDb = this.getPhysicalDatabaseFromIDInstance(origDbId, "MAIN");
		// logicalDatabaseId is common
		int ldbId = origDb.getLogicalDatabaseId();
		// consumed name is on original
		String name = this.calculateInstanceName(origDb, newInstance);
		// create the new record
		OrdsPhysicalDatabase newDb = new OrdsPhysicalDatabase();
		newDb.setLogicalDatabaseId(ldbId);
		newDb.setDatabaseServer(origDb.getDatabaseServer());
		newDb.setImportProgress(OrdsPhysicalDatabase.ImportType.FINISHED);
		newDb.setDbConsumedName(name);
		EntityType type;
		if ( newInstance.equalsIgnoreCase("MILESTONE")) {
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
		
		if (this.checkDatabaseExists(name)) {
			String statement = this.getTerminateStatement(name);
			this.runSQLQuery(statement, null, null, null);
			statement = "rollback transaction; drop database " + name + ";";
			this.runSQLStatementOnOrdsDB(statement);
		}
		String clonedb = String.format(
				"ROLLBACK TRANSACTION; CREATE DATABASE %s WITH TEMPLATE %s OWNER = %s",
				quote_ident(name),
				quote_ident(origDb.getDbConsumedName()),
				quote_ident(userName));
		this.runSQLStatementOnOrdsDB(clonedb);

		return newDb;
	
	}

	private OrdsPhysicalDatabase createEmptyDatabase(int id, DatabaseRequest databaseDTO, String instance,
			String userName, String password) throws Exception {
		log.debug("createEmptyDatabase");
		OrdsPhysicalDatabase db = new OrdsPhysicalDatabase();
		if ( id != 0 ) {
			db.setPhysicalDatabaseId(id);
		}
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
		} else {
			String dbName = db.getDbConsumedName();
			String statement = String.format(
					"rollback transaction;create database %s owner = \"%s\";",
					dbName, userName);

			this.runSQLStatementOnOrdsDB(statement);
			String createSequence = "CREATE SEQUENCE ords_constraint_seq";
			String server = db.getDatabaseServer();
			this.runJDBCQuery(createSequence, null, server, dbName);

			return db;
		}
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
	
	private String calculateInstanceName ( OrdsPhysicalDatabase db, String instance ){
		return 	(instance + "_" + db.getPhysicalDatabaseId() + "_" + db.getLogicalDatabaseId()).toLowerCase();
	}

	@Override
	public OrdsDB getLogicalDatabase(int dbId) throws Exception {

		Transaction transaction = null;
		Session session = this.getOrdsDBSessionFactory().openSession();
		OrdsDB ordsdb = null;
		try {
			transaction = session.beginTransaction();
			ordsdb = (OrdsDB) session.get(OrdsDB.class, dbId);
			transaction.commit();
		} catch (HibernateException e) {
			log.error("Run time exception", e);
			if (transaction != null && transaction.isActive()) {
				try {
					transaction.rollback();
				} catch (HibernateException e1) {
				}
				throw e;
			}
			return null;
		} finally {
			session.close();
		}
		return ordsdb;
	}

}