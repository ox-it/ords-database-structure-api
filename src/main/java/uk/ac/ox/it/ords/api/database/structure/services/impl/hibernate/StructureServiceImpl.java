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

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.shiro.SecurityUtils;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.structure.services.impl.AbstractStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.HibernateUtils;
import uk.ac.ox.it.ords.api.database.structure.model.User;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;

public class StructureServiceImpl extends AbstractStructureService {
	Logger log = LoggerFactory.getLogger(StructureServiceImpl.class);
	protected static String ODBC_MASTER_PASSWORD_PROPERTY = "ords.odbc.masterpassword";
	protected static String ORDS_DATABASE_NAME = "ords.database.name";
	protected static String ORDS_DATABASE_USER = "ords.database.user";
	protected static String ORDS_DATABASE_PASSWORD = "ords.database.password";
	protected static String ORDS_DATABASE_HOST = "ords.database.server.host";

	private SessionFactory sessionFactory;

	private void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	/**
	 * Class constructor - creates the session factory for accessing the ords
	 * database
	 */
	public StructureServiceImpl() {
		setSessionFactory(HibernateUtils.getSessionFactory());
	}

	/**
	 * Gets the session factory for accessing the ords database
	 * 
	 * @return the session factory
	 */
	public SessionFactory getOrdsDBSessionFactory() {
		return sessionFactory;
	}

	/**
	 * Gets the session factory for a user's database
	 * 
	 * @param dbName the database
	 * @param userName the user 
	 * @param password the password
	 * @return SessionFactory object
	 */
	public SessionFactory getUserDBSessionFactory(String dbName,
			String userName, String password) {
		return HibernateUtils.getUserDBSessionFactory(dbName, userName,
				password);

	}

	/**
	 * Returns the odbc username for the currently signed in user
	 * 
	 * @return the ODBC user name
	 * @throws Exception if there is a problem
	 */
	public String getODBCUserName() throws Exception {
		String principalName = SecurityUtils.getSubject().getPrincipal()
				.toString();
		User u = this.getUserByPrincipal(principalName);
		return u.calculateOdbcUserForOrds();
	}

	/**
	 * Returns the odbc password for the currently signed in user
	 * 
	 * @return the ODBC password
	 * @throws ConfigurationException if there is a problem
	 */
	public String getODBCPassword() throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				StructureServiceImpl.ODBC_MASTER_PASSWORD_PROPERTY);
	}
	
	
	public String getORDSDatabaseUser() throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				StructureServiceImpl.ORDS_DATABASE_USER);
	}
	
	
	public String getORDSDatabasePassword()  throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				StructureServiceImpl.ORDS_DATABASE_PASSWORD);
	}

	public String getORDSDatabaseName() throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				StructureServiceImpl.ORDS_DATABASE_NAME);
	}
	
	
	public String getORDSDatabaseHost()  throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				StructureServiceImpl.ORDS_DATABASE_HOST);
	}

	public boolean checkDatabaseExists(String databaseName) throws Exception {
		String sql = "SELECT COUNT(*) as count from pg_database WHERE datname = ?";
		List<Object> parameters = this.createParameterList(databaseName);
		return this.runCountSql(sql, parameters,null, null, null, null) == 1;
		/*
		 * Session session = this.getOrdsDBSessionFactory().getCurrentSession();
		 * try { Transaction transaction = session.beginTransaction(); SQLQuery
		 * query = session.createSQLQuery(sql);
		 * 
		 * @SuppressWarnings("rawtypes") List results = query.list();
		 * transaction.commit(); if(results.size() == 0 ) { return false; } else
		 * { return true; } } catch (Exception e) { log.debug(e.getMessage());
		 * session.getTransaction().rollback(); throw e; }
		 */}

	/**
	 * Returns true if the table exists in the given database
	 * 
	 * @param tableName the table
	 * @param databaseName the database
	 * @param databaseServer the database server
	 * @param userName the user
	 * @param password the user's password
	 * @return true if the table exists
	 * @throws Exception if there is a problem performing the check
	 */
	public boolean checkTableExists(String tableName, String databaseName, String databaseServer,
			String userName, String password) throws Exception {
		String sql = "SELECT COUNT(*) as count FROM pg_class WHERE relname=?";
		List<Object> parameters = this.createParameterList(tableName);
		return runCountSql(sql, parameters, databaseName, databaseServer, userName, password) == 1;
	}

	/**
	 * Returns true if the column exists in the given table and database
	 * 
	 * @param columnName the column
	 * @param tableName the table
	 * @param databaseName the database
	 * @param databaseServer the database server
	 * @param userName the user
	 * @param password the user's password
	 * @return true if the column exists
	 * @throws Exception if there is a problem performing the check
	 */
	public boolean checkColumnExists(String columnName, String tableName,
			String databaseName, String databaseServer, String userName, String password)
			throws Exception {
		String sql = "SELECT COUNT(*) as count FROM information_schema.columns "
				+ "WHERE table_catalog=? AND table_schema=? AND table_name=? AND column_name=?";
		List<Object> parameters = this.createParameterList(databaseName,
				"public", tableName, columnName);
		return runCountSql(sql, parameters, databaseName, databaseServer, userName, password) == 1;
	}

	public boolean checkConstraintExists(String tableName,
			String constraintName, String databaseName, String databaseServer, String userName,
			String password) throws Exception {
		String query = "SELECT COUNT(*) as count FROM information_schema.table_constraints "
				+ "WHERE table_catalog=? AND table_schema=? AND table_name=? AND constraint_name=?;";
		List<Object> parameters = this.createParameterList(databaseName,
				"public", tableName, constraintName);
		return runCountSql(query, parameters, databaseName, databaseServer, userName, password) == 1;

	}

	public boolean checkIndexExists(String tableName, String indexName,
			String databaseName, String databaseServer, String userName, String password)
			throws Exception {
		String query = "SELECT COUNT(*) FROM pg_index as idx JOIN pg_class as i ON i.oid = idx.indexrelid "
				+ "WHERE CAST(idx.indrelid::regclass as text) = ? AND relname = ?";
		List<Object> parameters = this
				.createParameterList(tableName, indexName);
		return runCountSql(query, parameters, databaseName, databaseServer, userName, password) == 1;

	}

	private int runCountSql(String sql, List<Object> parameters, String dbName, String databaseServer,
			String username, String password) throws Exception {
		CachedRowSet result = this
				.runJDBCQuery(sql, parameters, databaseServer, dbName);
		try {
			// If count is 1, then a table with the given name was found
			while (result.next()) {
				return result.getInt("count");
			}
		} finally {
			if ( result != null ) result.close();
		}
		return 0;

	}
	
	
	protected void runSQLStatementOnOrdsDB(String statement) {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery query = session.createSQLQuery(statement);
			query.executeUpdate();
			transaction.commit();
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

//	protected void runSQLStatement(String statement, String databaseName,
//			String userName, String password) {
//		Session session;
//		if (databaseName == null) {
//			session = this.getOrdsDBSessionFactory().openSession();
//		} else {
//			session = this.getUserDBSessionFactory(databaseName, userName,
//					password).openSession();
//		}
//		try {
//			Transaction transaction = session.beginTransaction();
//			SQLQuery query = session.createSQLQuery(statement);
//			query.executeUpdate();
//			transaction.commit();
//		} catch (Exception e) {
//			log.debug(e.getMessage());
//			session.getTransaction().rollback();
//			throw e;
//		} finally {
//			session.close();
//		}
//	}

	protected Object singleResultQuery(String query, String databaseName,
			String userName, String password) throws Exception {

		Session session;
		if (databaseName == null) {
			session = this.getOrdsDBSessionFactory().openSession();
		} else {
			session = this.getUserDBSessionFactory(databaseName, userName,
					password).openSession();
		}
		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery sqlQuery = session.createSQLQuery(query);
			Object result = sqlQuery.uniqueResult();
			transaction.commit();
			return result;
		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}



	protected void saveModelObject(Object objectToSave) throws Exception {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			session.save(objectToSave);
			transaction.commit();
		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}
	
	
	protected void updateModelObject(Object objectToUpdate ) throws Exception {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			session.update(objectToUpdate);
			transaction.commit();
		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}

	protected void removeModelObject(Object objectToRemove) throws Exception {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			session.delete(objectToRemove);
			transaction.commit();
		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}
	


	@SuppressWarnings("rawtypes")
	protected List runSQLQuery(String query, String databaseName,
			String username, String password) {
		Session session;
		if (databaseName == null) {
			session = this.getOrdsDBSessionFactory().openSession();
		} else {
			session = this.getUserDBSessionFactory(databaseName, username,
					password).openSession();
		}
		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery sqlQuery = session.createSQLQuery(query);
			List results = sqlQuery.list();
			transaction.commit();
			return results;

		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}

	protected String calculateStagingName(String dbName) {
		return dbName + "_staging";
	}

	protected String columnComment(String databaseName, String databaseServer, String tableName,
			String columnName) throws Exception {
		log.debug("columnComment");
		// col_description gives the comment stored for the given column.
		// As with obj_description, we need the oid of the table which we
		// acheieve
		// by casting to regclass then to oid. We also need the column number
		// within the table, which we get from the name by using a subquery to
		// look it up in pg_attribute (Again requiring the table oid).
		String query = "SELECT col_description(quote_ident(?)::regclass::oid, (SELECT attnum FROM pg_attribute WHERE attrelid = quote_ident(?)::regclass::oid AND attname = ?)) as comment";


		ArrayList<Object> parameters = new ArrayList<Object>();

		parameters.add(tableName);
		parameters.add(tableName);
		parameters.add(columnName);
		String comment = "";
		CachedRowSet result = this.runJDBCQuery(query, parameters, databaseServer,
				databaseName);
		if (result == null) {
			return comment;
		}
		try {
			while (result.next()) {
				comment = result.getString("comment");
			}
		} finally {
			result.close();
		}
		return comment;
	}

	protected String tableComment(String databaseName, String databaseServer, String tableName)
			throws Exception {
		log.debug("tableComment");
		// obj_description gives the comment for an object, but needs the unique
		// oid for that object. To find the oid for a table from its name, we
		// need to to cast the table ident to regclass then to oid. 'pg_class'
		// is the catalog the table belongs to.
		String identifier = String.format( "\'public.%s\'", quote_ident(tableName));
		String query = String.format("SELECT obj_description(%s::regclass::oid, 'pg_class') as comment",
				identifier);

		String comment = "";
		CachedRowSet result = this.runJDBCQuery(query, null, databaseServer,
				databaseName);
		if (result == null) {
			return comment;
		}
		try {
			while (result.next()) {
				comment = result.getString("comment");
			}
		} finally {
			result.close();
		}
		return comment;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	protected TableList addTableMetadata(String databaseName, String databaseServer, String tableName,
			TableList response) throws Exception {
		log.debug("addTableMetadata");
		// Get the foriegn keys, indexes, stored position and comment
		// For each table.
		List<HashMap<String, String>> foreignKeys = this
				.getForeignKeysFromPostgres(databaseName, databaseServer, tableName);
		List<HashMap<String, Object>> indexes = this.getIndexesFromPostgres(
				databaseName, databaseServer, tableName);

		log.debug(tableName);
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		// Get a complete description of the table columns
		List<HashMap<String, String>> columns = this.getTableDescription(
				databaseName, tableName, databaseServer, userName, password);

		if (log.isDebugEnabled()) {
			log.debug(String.format("We have %d rows", columns.size()));
		}

		for (HashMap<String, String> column : columns) {
			String columnName = column.get("column_name");
			String defaultValue = column.get("column_default");
			int position = Integer.parseInt(column.get("ordinal_position"));
			boolean nullable = column.get("is_nullable").compareToIgnoreCase(
					"YES") == 0;
			String columnComment = columnComment(databaseName, databaseServer, tableName,
					columnName);

			Integer autoIncrement = 0;
			// Parse the default value to an interface-friendly
			// format, and identify if the field is auto-incremented
			if (defaultValue != null) {
				if (defaultValue.equals("''::text")) { // CSV
					defaultValue = "";
				} else if (defaultValue
						.matches("nextval\\('[A-Za-z0-9_\"]+'::regclass\\)")) {
					defaultValue = "";
					autoIncrement = 1;
				} else if (defaultValue.startsWith("NULL::")) {
					defaultValue = null;
				}
			}

			// Format the field size appropriately for the data type
			String fieldSize = getFieldSize(column);

			// Translate the data type from PostgreSQL-speak to
			// Schema Designer-speak
			String dataType = SqlDesignerTranslations.translateDatatype(column
					.get("data_type").toUpperCase(), fieldSize);

			// Add the column to the response
			response.addColumn(tableName, columnName, position, defaultValue,
					nullable, dataType, autoIncrement == 1, columnComment);
		}

		// Use each foreign key to add table relationships to the
		// response
		for (HashMap<String, String> foreignKey : foreignKeys) {
			ArrayList parts = new ArrayList();
			parts.add(foreignKey.get("columnName"));

			//
			// Get a subset of column information for the related table
			//
			HashMap<String, HashMap<String, String>> foreignTableColumnMap = new HashMap<String, HashMap<String, String>>();
			List<HashMap<String, String>> foreignTableColumns = this
					.getTableDescription(databaseName,
							(String) foreignKey.get("foreignTableName"), databaseServer,
							userName, password);
			if (foreignTableColumns != null) {
				for (HashMap entry : foreignTableColumns) {
					HashMap<String, String> column = new HashMap<String, String>();
					column.put(
							"datatype",
							SqlDesignerTranslations.translateDatatype(
									((String) entry.get("data_type"))
											.toUpperCase(), getFieldSize(entry)));
					foreignTableColumnMap.put(
							(String) entry.get("column_name"), column);
				}
			}

			response.addRelation(tableName,
					(String) foreignKey.get("constraintName"),
					(String) foreignKey.get("columnName"),
					(String) foreignKey.get("foreignTableName"),
					(String) foreignKey.get("foreignColumnName"),
					foreignTableColumnMap);
		}

		// Add each index to the response
		if (indexes.size() > 0) {
			for (HashMap<String, Object> index : indexes) {
				response.addIndex(tableName, (String) index.get("name"),
						(String) index.get("type"), (List) index.get("columns"));
			}
		}
		return response;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public List<HashMap<String, String>> getForeignKeysFromPostgres(
			String databaseName,String databaseServer, String table) throws Exception {
		log.debug("getForeignKeysFromPostgres");

		String query = "SELECT "
				+ "tc.constraint_name, tc.table_name, kcu.column_name, "
				+ "ccu.table_name AS foreign_table_name, "
				+ "ccu.column_name AS foreign_column_name "
				+ "FROM information_schema.table_constraints tc "
				+ "JOIN information_schema.key_column_usage kcu "
				+ "ON tc.constraint_name = kcu.constraint_name "
				+ "JOIN information_schema.constraint_column_usage ccu "
				+ "ON ccu.constraint_name = tc.constraint_name " + "WHERE "
				+ "constraint_type = 'FOREIGN KEY' " + "AND tc.table_name = ?";

		List foreignKeys = new ArrayList();
		HashMap<String, String> foreignKey;
		ArrayList<Object> parameters = new ArrayList<Object>();
		parameters.add(table);
		CachedRowSet rs = this.runJDBCQuery(query, parameters, databaseServer,
				databaseName);
		// List<Object[]> results = this.runSQLQuery(query, null, null, null);
		while (rs.next()) {
			foreignKey = new HashMap();
			foreignKey.put("constraintName", rs.getString("constraint_name"));
			foreignKey.put("tableName", rs.getString("table_name"));
			foreignKey.put("columnName", rs.getString("column_name"));
			foreignKey.put("foreignTableName",
					rs.getString("foreign_table_name"));
			foreignKey.put("foreignColumnName",
					rs.getString("foreign_column_name"));

			foreignKeys.add(foreignKey);
		}

		return foreignKeys;
	}

	protected List<HashMap<String, Object>> getIndexesFromPostgres(
			String databaseName, String databaseServer, String table) throws Exception {
		String query = "SELECT " + "i.relname as indexname, "
				+ "idx.indrelid::regclass as tablename, " + "ARRAY( "
				+ "SELECT pg_get_indexdef(idx.indexrelid, k + 1, true) "
				+ "FROM generate_subscripts(idx.indkey, 1) as k "
				+ "ORDER BY k " + ") as colnames, "
				+ "indisunique as isunique, " + "indisprimary as isprimary "
				+ "FROM " + "pg_index as idx " + "JOIN pg_class as i "
				+ "ON i.oid = idx.indexrelid "
				+ "WHERE CAST(idx.indrelid::regclass as text) = quote_ident(?)";
		List<HashMap<String, Object>> indexes = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> index;
		String type;
		ArrayList<Object> parameters = new ArrayList<Object>();
		parameters.add(table);
		CachedRowSet rs = this.runJDBCQuery(query, parameters, databaseServer,
				databaseName);
		// List<Object[]> results = this.runSQLQuery(command, null, null, null);
		while (rs.next()) {
			index = new HashMap<String, Object>();
			index.put("name", rs.getString("indexname"));
			ArrayList<String> columns = new ArrayList<String>();
			Array sqlArray = rs.getArray("colnames");
			Object[] cols = (Object[]) sqlArray.getArray();
			// ResultSet columnSet = sqlArray.getResultSet();
			for (Object column : cols) {
				//
				// PG may store the index columns as quoted identifiers, in which case we need
				// to unquote them to return via the API
				//
				columns.add(unquote(column.toString()));
			}
			index.put("columns", columns);
			if (rs.getBoolean("isprimary")) {
				type = "PRIMARY";
			} else if (rs.getBoolean("isunique")) {
				type = "UNIQUE";
			} else {
				type = "INDEX";
			}
			index.put("type", type);

			indexes.add(index);
		}

		return indexes;
	}

	public List<HashMap<String, String>> getTableDescription(
			String databaseName, String tableName, String server, String userName,
			String password) throws Exception {
		log.debug("getTableDescription");

		ArrayList<String> fields = new ArrayList<String>();
		fields.add("column_name");
		fields.add("data_type");
		fields.add("character_maximum_length");
		fields.add("numeric_precision");
		fields.add("numeric_scale");
		fields.add("column_default");
		fields.add("is_nullable");
		fields.add("ordinal_position");

		String query = String
				.format("select %s from INFORMATION_SCHEMA.COLUMNS where table_name = %s ORDER BY ordinal_position ASC",
						StringUtils.join(fields.iterator(), ","),
						quote_literal(tableName));

		HashMap<String, String> columnDescription;
		List<HashMap<String, String>> columnDescriptions = new ArrayList<HashMap<String, String>>();
		CachedRowSet results = this.runJDBCQuery(query, null, server, databaseName);

		// First get all column names
		while (results.next()) {
			columnDescription = new HashMap<String, String>();
			for (String field : fields) {
				Object c = results.getObject(field);
				if (c != null) {
					columnDescription.put(field, c.toString());
				} else {
					columnDescription.put(field, null);
				}
			}
			columnDescriptions.add(columnDescription);
		}

		return columnDescriptions;
	}

	protected String getFieldSize(HashMap<String, String> column) {
		log.debug("getFieldSize");
		// Format the field size appropriately for the data type
		String fieldSize;
		if (column.get("data_type").compareToIgnoreCase("numeric") == 0) {
			fieldSize = String.format("%s,%s", column.get("numeric_precision"),
					column.get("numeric_scale"));
		} else {
			fieldSize = column.get("character_maximum_length");
		}
		return fieldSize;
	}

	protected User getUserByPrincipal(String principalName) {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();

			@SuppressWarnings("unchecked")
			List<User> users = (List<User>) session.createCriteria(User.class)
					.add(Restrictions.eq("principalName", principalName))
					.list();
			transaction.commit();
			if (users.size() == 1) {
				return users.get(0);
			}
			return null;
		} catch (Exception e) {
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}

	protected String getTerminateStatement(String databaseName)
			throws Exception {
		boolean above9_2 = isPostgresVersionAbove9_2();
		String command;
		if (above9_2) {
			log.info("Postgres version is 9.2 or later");
			command = String
					.format("select pg_terminate_backend(pid) from pg_stat_activity where datname = '%s' AND pid <> pg_backend_pid()",
							databaseName);
		} else {
			log.info("Postgres version is earlier than 9.2");
			command = String
					.format("select pg_terminate_backend(procpid) from pg_stat_activity where datname = '%s' AND procpid <> pg_backend_pid()",
							databaseName);
		}
		return command;

	}

	private String[] getPostgresVersionArray() throws Exception {

		String version = (String) this.singleResultQuery("SELECT version()",
				null, null, null);

		String[] versionArray = null;
		String[] tempVersionArray = null;
		tempVersionArray = version.split(" ");
		version = tempVersionArray[1];
		versionArray = version.split("\\.");

		return versionArray;
	}

	public boolean isPostgresVersionAbove9_2() throws Exception {
		String[] versionArray = getPostgresVersionArray();
		boolean above = false;
		if (versionArray != null) {
			try {
				int majorVersionNumber = Integer.parseInt(versionArray[0]);
				int minorVersionNumber = Integer.parseInt(versionArray[1]);
				if (majorVersionNumber >= 9) {
					if (minorVersionNumber >= 2) {
						above = true;
					}
				}
			} catch (NumberFormatException e) {
				log.error("Unable to get Postgres version");
			}
		}

		return above;
	}

	/**
	 * createParametersList: convenience function for creating a list
	 * 
	 * @param args the list of parameters
	 * @return List of parameters
	 */
	protected List<Object> createParameterList(Object... args) {
		ArrayList<Object> parameters = new ArrayList<Object>();
		for (Object p : args) {
			parameters.add(p);
		}
		return parameters;
	}
	
	protected void runSQLStatements(List<String> statements, String server,
			String databaseName) throws Exception {
		Connection connection = null;
		Properties connectionProperties = new Properties();
		PreparedStatement preparedStatement = null;
		if ( server != null ){
			
			String userName = this.getODBCUserName();
			String password = this.getODBCPassword();
			connectionProperties.put("user", userName);
			connectionProperties.put("password", password);
		}
		else {
			// get the ords database configuration
			Configuration config = MetaConfiguration.getConfiguration();
			connectionProperties.put("user", config.getString(StructureServiceImpl.ORDS_DATABASE_USER));
			connectionProperties.put("password", config.getString(StructureServiceImpl.ORDS_DATABASE_PASSWORD));
			server = config.getString(StructureServiceImpl.ORDS_DATABASE_HOST);
			databaseName = this.getORDSDatabaseName();
			if ( server == null ) {
				server = "localhost";
			}
		}
		String connectionURL = "jdbc:postgresql://" + server + "/"
				+ databaseName;
		try {
			connection = DriverManager.getConnection(connectionURL,
					connectionProperties);
			for (String statement: statements ) {
				preparedStatement = connection.prepareStatement(statement);
				preparedStatement.execute();
			}
		}
		finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}

	protected CachedRowSet runJDBCQuery(String query, List<Object> parameters,
			String server, String databaseName) throws Exception {
		Connection connection = null;
		Properties connectionProperties = new Properties();
		PreparedStatement preparedStatement = null;
		if ( server != null && databaseName != null ){
			
			String userName = this.getODBCUserName();
			String password = this.getODBCPassword();
			connectionProperties.put("user", userName);
			connectionProperties.put("password", password);
		}
		else {
			// get the ords database configuration
			//Configuration config = MetaConfiguration.getConfiguration();
			connectionProperties.put("user", this.getORDSDatabaseUser());
			connectionProperties.put("password", this.getORDSDatabasePassword());
			if ( server == null ) {
				server = this.getORDSDatabaseHost();
			}
			if (databaseName == null ) {
				databaseName = this.getORDSDatabaseName();
			}
		}
		String connectionURL = "jdbc:postgresql://" + server + "/"
				+ databaseName;
		try {
			connection = DriverManager.getConnection(connectionURL,
					connectionProperties);
			preparedStatement = connection.prepareStatement(query);
			if (parameters != null) {
				int paramCount = 1;
				for (Object parameter : parameters) {
					@SuppressWarnings("rawtypes")
					Class type = parameter.getClass();
					if (type.equals(String.class)) {
						preparedStatement.setString(paramCount,
								(String) parameter);
					}
					if (type.equals(Integer.class)) {
						preparedStatement.setInt(paramCount,
								(Integer) parameter);
					}
					paramCount++;
				}

			}
			if (query.toLowerCase().startsWith("select")) {
				ResultSet result = preparedStatement.executeQuery();
				CachedRowSet rowSet = RowSetProvider.newFactory()
						.createCachedRowSet();
				rowSet.populate(result);
				log.debug("prepareAndExecuteStatement:return result");
				return rowSet;
			} else {
				preparedStatement.execute();
				log.debug("prepareAndExecuteStatement:return null");
				return null;
			}

		} catch (SQLException e) {
			log.error("Error with this command", e);
			log.error("Query:" + query);
			throw e;
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
		}

	}

}
