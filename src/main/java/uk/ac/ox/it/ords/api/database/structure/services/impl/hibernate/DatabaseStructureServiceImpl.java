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


import java.sql.SQLException;
import java.util.List;

import javax.ws.rs.NotFoundException;

import org.apache.shiro.SecurityUtils;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;

import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase.EntityType;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;
import uk.ac.ox.it.ords.api.user.model.User;
import uk.ac.ox.it.ords.api.user.services.UserService;
import uk.ac.ox.it.ords.api.database.structure.model.SchemaDesignerTable;

public class DatabaseStructureServiceImpl extends StructureServiceImpl
		implements
			DatabaseStructureService {

	@Override
	public List<OrdsPhysicalDatabase> getDatabaseList() throws Exception {
		String principalName = SecurityUtils.getSubject().getPrincipal()
				.toString();
		UserService userService = UserService.Factory.getInstance();

		User u = userService.getUserByPrincipalName(principalName);
		
		Session session = this.getOrdsDBSessionFactory().getCurrentSession();
		try {
			Transaction transaction = session.beginTransaction();
			@SuppressWarnings("unchecked")
			List<OrdsPhysicalDatabase> dbs = session.createCriteria(OrdsPhysicalDatabase.class).
					add(Restrictions.eq("actorId", u.getUserId())).
					list();
			transaction.commit();
			return dbs;
		}
		catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		}
		finally {
			session.close();
		}

	}

	@Override
	public OrdsPhysicalDatabase createNewDatabase(int logicalDBId,
			String instance) throws Exception {
		String username = this.getODBCUserName();
		String password = this.getODBCPassword();
		this.createOBDCUserRole(username, password);
		OrdsPhysicalDatabase db = this.createEmptyDatabase(logicalDBId, username, password);
		return db;
	}


	@Override
	public OrdsPhysicalDatabase getDatabaseMetaData(int dbId, String instance)
			throws Exception {
		Session session = this.getOrdsDBSessionFactory().getCurrentSession();
		try {
			Transaction transaction = session.beginTransaction();
			EntityType dbType = OrdsPhysicalDatabase.EntityType.valueOf(instance
					.toUpperCase());
			@SuppressWarnings("unchecked")
			List<OrdsPhysicalDatabase> dbs = session.createCriteria(OrdsPhysicalDatabase.class)
					.add(Restrictions.eq("physicalDatabaseId", dbId))
							.add(Restrictions.eq("entityType", dbType)).list();
			transaction.commit();
			if ( dbs.size() != 1 ) {
				throw new NotFoundException("Cannot find physical database id "+dbId);
			}
			return dbs.get(0);
		}
		catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		}
		finally {
			session.close();
		}
	}

	@Override
	public TableList getDatabaseTableList(int dbId, String instance, boolean staging)
			throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		// we need to get the physical database this time because we need it's id later
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName;
		if ( !staging ) {
			databaseName = database.getDbConsumedName();
		}
		else {
			databaseName = this.calculateStagingName(database.getDbConsumedName());
		}

		String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name";
		// getting a single scalar value so hibernate returns a list of strings
		List results = this.runSQLQuery(query, databaseName, userName, password);
		TableList tables = new TableList();
		
        int counter = 0;
        int multiplier; // Convenience variables for displaying the tables
        // in slightly less cramped positions on the grid
        if (results.size() > 50) {
            multiplier = 1;
        }
        else if (results.size() > 25) {
            multiplier = 50;
        }
        else {
            multiplier = 120;
        }

		for ( Object tableNameObject: results) {
			String tableName = tableNameObject.toString();
			// get the schema designer table for this table
			SchemaDesignerTable sdt = this.getSchemaDesignerTable(database.getPhysicalDatabaseId(), tableName.toString());
			String comment = this.tableComment(tableName, databaseName, userName, password);
			tables.addTable(tableName, comment);
            addTableMetadata(tableName, tables);
            if (sdt == null) {
                tables.setXY(tableName, counter*multiplier, counter*multiplier);
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
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String stagingName = this.calculateStagingName(database.getDbConsumedName());
		if (this.checkDatabaseExists(stagingName) ) {
			this.deleteDatabase(dbId, instance, true);
		}
        String clonedb = String.format("CREATE DATABASE %s WITH TEMPLATE %s", 
                quote_ident(stagingName), 
                quote_ident(database.getDbConsumedName()));
        this.runSQLStatement(clonedb, null, null, null);
        String sequenceName = "ords_constraint_seq";
        String createSequence = String.format("CREATE SEQUENCE %s",
                                                quote_ident(sequenceName));
        this.runSQLStatement(createSequence, stagingName, userName, password);
		return stagingName;
	}

	@Override
	public void updateStagingDatabase(int dbId, OrdsPhysicalDatabase update) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void mergeStagingToActual(int dbId, String instance) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		String stagingName = this.calculateStagingName(databaseName);
		
		if ( !this.checkDatabaseExists(database.getDbConsumedName())) {
			throw new NotFoundException("Original database does not exist");
		}
		deleteDatabase(dbId, instance, false);
		String sql = String.format("ALTER DATABASE %s RENAME TO %s", stagingName, databaseName);
		this.runSQLStatement(sql, null, null, null);

	}

	@Override
	public void deleteDatabase(int dbId, String instance, boolean staging) throws Exception {
		// TODO Auto-generated method stub
		String databaseName = this.dbNameFromIDInstance(dbId, instance, staging);
		String sql = "drop database " + databaseName;
		this.runSQLQuery(sql, null, null, null);
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
			this.runSQLStatement(command, null, null, null);
		}
	}

	private OrdsPhysicalDatabase createEmptyDatabase(int logicalDatabaseId, String userName,
			String password) throws Exception {
		log.debug("createEmptyDatabase");
		OrdsPhysicalDatabase db = new OrdsPhysicalDatabase();
		db.setLogicalDatabaseId(logicalDatabaseId);
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
					"create database %s owner = \"%s\"", dbName, userName);

			this.runSQLStatement(statement, null, null, null);
			return db;
		}
	}
	
	private SchemaDesignerTable getSchemaDesignerTable(int databaseId, String tableName) {
		log.debug("getTable:" + databaseId +","+tableName);
        Transaction tx = null;
        Session session = this.getOrdsDBSessionFactory().getCurrentSession();
        try {
            tx = session.beginTransaction();
            SQLQuery q = session.createSQLQuery("SELECT * FROM schemadesignertable WHERE databaseid=:databaseid AND tablename=:tablename")
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
        }
	}
	


	
	

}