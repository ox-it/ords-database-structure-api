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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.shiro.SecurityUtils;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.HibernateUtils;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase.EntityType;
import uk.ac.ox.it.ords.api.database.structure.conf.MetaConfiguration;
import uk.ac.ox.it.ords.api.user.model.User;
import uk.ac.ox.it.ords.api.user.services.UserService;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;

public class StructureServiceImpl {
	Logger log = LoggerFactory.getLogger(StructureServiceImpl.class);

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
	 * @return
	 */
	public SessionFactory getOrdsDBSessionFactory() {
		return sessionFactory;
	}

	/**
	 * Gets the session factory for a user's database
	 * 
	 * @param dbId
	 * @param instance
	 * @param userName
	 * @param password
	 * @return SessionFactory object
	 */
	public SessionFactory getUserDBSessionFactory(String dbName,
			String userName, String password) {
		return HibernateUtils.getUserDBSessionFactory(dbName, userName,
				password);

	}
	
	
	protected OrdsPhysicalDatabase getPhysicalDatabaseFromIDInstance ( int dbId, String instance){
		EntityType dbType = OrdsPhysicalDatabase.EntityType.valueOf(instance
				.toUpperCase());
		OrdsPhysicalDatabase database = this
				.getPhysicalDatabaseByLogicalDatabaseId(dbId, dbType);
		return database;
	}

	public String dbNameFromIDInstance(int dbID, String instance, boolean staging) {
		OrdsPhysicalDatabase database = getPhysicalDatabaseFromIDInstance(dbID, instance);
		if ( !staging ) {
			return database.getDbConsumedName();
		}
		else {
			return this.calculateStagingName(database.getDbConsumedName());
		}
	}

	private OrdsPhysicalDatabase getPhysicalDatabaseByLogicalDatabaseId(
			int logicalDatabaseId, EntityType entityType) {
		if (log.isDebugEnabled()) {
			log.debug(String.format(
					"getPhysicalDatabaseByLogicalDatabaseId(%d, %s)",
					logicalDatabaseId, entityType));
		}

		Transaction tx = null;
		Session session = this.sessionFactory.getCurrentSession();
		try {
			tx = session.beginTransaction();
			SQLQuery q = session
					.createSQLQuery(
							"select * from ordsPhysicalDatabase where logicalDatabaseId=:logicalDatabaseId and entityType=:entityType")
					.addEntity(OrdsPhysicalDatabase.class);
			q.setParameter("logicalDatabaseId", logicalDatabaseId);
			q.setParameter("entityType", entityType.ordinal());
			@SuppressWarnings("unchecked")
			List<OrdsPhysicalDatabase> databases = q.list();
			tx.commit();
			if ((databases != null) && (!databases.isEmpty())) {
				log.debug("return item");
				if (databases.size() > 1) {
					log.error(String
							.format("Too many entries (%d) exist for this. Selecting one of them. This should never happen!",
									databases.size()));
				}
				return databases.get(0);
			}
		} catch (HibernateException e) {
			log.error("Run time exception", e);
			if (tx != null && tx.isActive()) {
				try {
					tx.rollback();
				} catch (HibernateException e1) {
				}
				throw e;
			}
		}

		log.debug("getDatabase: return null");
		return null;
	}

	/**
	 * Returns the odbc username for the currently signed in user
	 * 
	 * @return
	 * @throws Exception
	 */
	public String getODBCUserName() throws Exception {
		String principalName = SecurityUtils.getSubject().getPrincipal()
				.toString();
		UserService userService = UserService.Factory.getInstance();

		User u = userService.getUserByPrincipalName(principalName);
		return u.calculateOdbcUserForOrds();
	}

	/**
	 * Returns the odbc password for the currently signed in user
	 * 
	 * @return
	 * @throws ConfigurationException
	 */
	public String getODBCPassword() throws ConfigurationException {
		String dbPropertiesLocation = MetaConfiguration
				.getConfigurationLocation("databaseProperties");
		PropertiesConfiguration configuration = new PropertiesConfiguration(
				dbPropertiesLocation);
		return configuration.getProperty("ordsOdbcUserMasterPassword")
				.toString();
	}
	
	
	public boolean checkDatabaseExists(String databaseName ) throws Exception {
		String sql = "SELECT 1 from pg_database WHERE datname = %s";
		sql = String.format(sql, quote_ident(databaseName));
		Session session = this.getOrdsDBSessionFactory().getCurrentSession();
		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery query = session.createSQLQuery(sql);
			@SuppressWarnings("rawtypes")
			List results = query.list();
			transaction.commit();
			if(results.size() == 0 ) {
				return false;
			}
			else {
				return true;
			}
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

	/**
	 * Returns true if the table exists in the given database
	 * 
	 * @param tableName
	 * @param databaseName
	 * @param userName
	 * @param password
	 * @return
	 */
	public boolean checkTableExists(String tableName, String databaseName,
			String userName, String password) {
		String sql = String.format(
				"SELECT COUNT(*) as count FROM pg_class WHERE relname=\'%s\'",
				tableName);
		return runCountSql(sql, databaseName, userName, password) == 1;
	}

	/**
	 * Returns true if the column exists in the given table and database
	 * 
	 * @param columnName
	 * @param tableName
	 * @param databaseName
	 * @param userName
	 * @param password
	 * @return
	 */
	public boolean checkColumnExists(String columnName, String tableName,
			String databaseName, String userName, String password) {
		String sql = String
				.format("SELECT COUNT(*) as count FROM information_schema.columns "
						+ "WHERE table_catalog=%s AND table_schema=%s AND table_name=%s AND column_name=%s",
						databaseName, "public", tableName, columnName);
		return runCountSql(sql, databaseName, userName, password) == 1;
	}
	
	
	public boolean checkConstraintExists(String tableName, String constraintName,
			String databaseName, String userName, String password) {
        String query = "SELECT COUNT(*) as count FROM information_schema.table_constraints "
                + "WHERE table_catalog=%s AND table_schema=%s AND table_name=%s AND constraint_name=%s;";
        query = String.format(query, quote_ident(databaseName),
        		"public",
        		quote_ident(tableName),
        		quote_ident(constraintName));
        return runCountSql(query, databaseName, userName, password) == 1;
 
	}
	
	
	
	public boolean checkIndexExists (String tableName, String indexName,
			String databaseName, String userName, String password ) {
        String query = "SELECT COUNT(*) FROM pg_index as idx JOIN pg_class as i ON i.oid = idx.indexrelid "           
                +"WHERE CAST(idx.indrelid::regclass as text) = %s AND relname = %s";
        query = String.format(query,
        		quote_ident(tableName),
        		quote_ident(indexName));
        return runCountSql(query,databaseName, userName, password) == 1;


	}

	private int runCountSql(String sql, String dbName, String username,
			String password) {
		Session session = this.getUserDBSessionFactory(dbName, username,
				password).getCurrentSession();
		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery query = session.createSQLQuery(sql);
			int count = ((Number)query.uniqueResult()).intValue();
			transaction.commit();
			return count;
		} 
		finally {
			session.close();
		}

	}
	
	
	protected void runSQLStatement(String statement, String databaseName, String userName, String password ) {
		Session session;
		if ( databaseName == null ) {
			session = this.getOrdsDBSessionFactory().getCurrentSession();
		}
		else {
			session = this.getUserDBSessionFactory(databaseName, userName, password).getCurrentSession();
		}
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
	
	
	protected Object singleResultQuery ( String query, String databaseName, String userName, String password){
		Session session;
		if ( databaseName == null ) {
			session = this.getOrdsDBSessionFactory().getCurrentSession();
		}
		else {
			session = this.getUserDBSessionFactory(databaseName, userName, password).getCurrentSession();
		}
		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery sqlQuery = session.createSQLQuery(query);
			Object result = sqlQuery.uniqueResult();
			transaction.commit();
			return result;
		}
		catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		}
	}
	
	
	protected void runSQLStatements(List<String> statements, String databaseName, String userName, String password ){
		Session session;
		if ( databaseName == null ) {
			session = this.getOrdsDBSessionFactory().getCurrentSession();
		}
		else {
			session = this.getUserDBSessionFactory(databaseName, userName, password).getCurrentSession();
		}
		try {
			Transaction transaction = session.beginTransaction();
			for ( String statement : statements ) { 
				SQLQuery query = session.createSQLQuery(statement);
				query.executeUpdate();
			}
			transaction.commit();
		}
		catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		}
	}
	
	
	protected void saveModelObject ( Object objectToSave ) throws Exception {
		Session session = this.getOrdsDBSessionFactory().getCurrentSession();
		try {
			Transaction transaction = session.beginTransaction();
			session.save(objectToSave);
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
	
	
	@SuppressWarnings("rawtypes")
	protected List runSQLQuery ( String query, String databaseName, String username, String password) {
		Session session;
		if (databaseName == null ) {
			session = this.getOrdsDBSessionFactory().getCurrentSession();
		}
		else {
			session = this.getUserDBSessionFactory(databaseName, username, password).getCurrentSession();
		}
		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery sqlQuery = session.createSQLQuery(query);
			List results = sqlQuery.list();
			transaction.commit();
			return results;
			
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

	protected String calculateStagingName(String dbName) {
		return dbName + "_staging";
	}

	/**
	 * Mimicks the postgres function, surrounding a table or column name in
	 * quotes, escaping existing quotes by doubling them.
	 * 
	 * @param ident
	 *            The table, column or other object name.
	 * @return
	 */
	protected String quote_ident(String ident) {
		return "\"" + ident.replace("\"", "\"\"") + "\"";
	}

	/**
	 * Mimicks the postgres function, surrounding a string in quotes, escaping
	 * existing quotes by doubling them.
	 * 
	 * @param literal
	 * @return
	 */
	protected String quote_literal(String literal) {
		if (literal == null) {
			return literal;
		}
		return "'" + literal.replace("'", "''") + "'";
	}
	
	
    protected String columnComment(String tableName, String columnName) throws SQLException, ClassNotFoundException {
        log.debug("columnComment");
        // col_description gives the comment stored for the given column.
        // As with obj_description, we need the oid of the table which we acheieve
        // by casting to regclass then to oid.  We also need the column number
        // within the table, which we get from the name by using a subquery to 
        // look it up in pg_attribute (Again requiring the table oid).
        String query = "SELECT col_description(quote_ident(%s)::regclass::oid, (SELECT attnum FROM pg_attribute WHERE attrelid = quote_ident(%s)::regclass::oid AND attname = %s)) as comment";
        
        query = String.format(query, quote_ident(tableName), quote_ident(tableName), quote_ident(columnName));
        
		Session session = this.getOrdsDBSessionFactory().getCurrentSession();

		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery sqlQuery = session.createSQLQuery(query);
			String comment = sqlQuery.uniqueResult().toString();
			transaction.commit();
			return comment;

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

	
	protected String tableComment(String tableName, String databaseName,
			String userName, String password) throws Exception {
		log.debug("tableComment");
		// obj_description gives the comment for an object, but needs the unique
		// oid for that object. To find the oid for a table from its name, we
		// need to to cast the table ident to regclass then to oid. 'pg_class'
		// is the catalog the table belongs to.
		String query = String
				.format("SELECT obj_description(quote_ident('%s')::regclass::oid, 'pg_class') as comment",
						tableName);
		Session session = this.getUserDBSessionFactory(databaseName, userName,
				password).getCurrentSession();

		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery sqlQuery = session.createSQLQuery(query);
			String comment = sqlQuery.uniqueResult().toString();
			transaction.commit();
			return comment;

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
	

	
	
    protected TableList addTableMetadata(String tableName, TableList response) throws Exception{
        log.debug("addTableMetadata");
    	// Get the foriegn keys, indexes, stored position and comment
    	// For each table.
    	List<HashMap<String, String>> foreignKeys = this.getForeignKeysFromPostgres(tableName);
    	List<HashMap<String, Object>> indexes = this.getIndexesFromPostgres(tableName);

    	log.debug(tableName);

    	// Get a complete description of the table columns
    	List<HashMap<String, String>> columns = this.getTableDescription(tableName);

    	if (log.isDebugEnabled()) {
    		log.debug(String.format("We have %d rows", columns.size()));
    	}

    	for (HashMap<String, String> column : columns) {
    		String columnName = column.get("column_name");
    		String defaultValue = column.get("column_default");
    		int position = Integer.parseInt(column.get("ordinal_position"));
    		boolean nullable = column.get("is_nullable").compareToIgnoreCase("YES") == 0;
    		String columnComment = columnComment(tableName, columnName);

    		Integer autoIncrement = 0;
    		// Parse the default value to an interface-friendly
    		// format, and identify if the field is auto-incremented
    		if (defaultValue != null) {
    			if (defaultValue.equals("''::text")) { // CSV
    				defaultValue = "";
    			} else if (defaultValue.matches("nextval\\('[A-Za-z0-9_\"]+'::regclass\\)")) {
    				defaultValue = "";
    				autoIncrement = 1;
    			} else if (defaultValue.startsWith("NULL::")) {
    				defaultValue = null;
    			}
    		}

    		if (log.isDebugEnabled()) {
    			log.debug("Name: "+columnName);
    			log.debug("datatype: "+ column.get("data_type"));
    			log.debug("default value:" + defaultValue);
    		}

    		// Format the field size appropriately for the data type
    		String fieldSize = getFieldSize(column);

    		// Translate the data type from PostgreSQL-speak to 
    		// Schema Designer-speak
    		String dataType = SqlDesignerTranslations
    				.translateDatatype(column.get("data_type").toUpperCase(),
    						fieldSize);

    		// Add the column to the response
    		response.addColumn(tableName, 
    				columnName, 
    				position, 
    				defaultValue, 
    				nullable, 
    				dataType, 
    				autoIncrement == 1,
    				columnComment);

    		if (log.isDebugEnabled()) {
    			log.debug(column.toString());
    		}
    	}

    	// Use each foreign key to add table relationships to the
    	// response
    	for (HashMap<String, String> foreignKey : foreignKeys) {
    		ArrayList parts = new ArrayList();
    		parts.add(foreignKey.get("columnName"));

    		//
    		// Get a subset of column information for the related table
    		//
    		HashMap<String, HashMap<String,String>> foreignTableColumnMap = new HashMap<String, HashMap<String,String>>();
    		List<HashMap<String, String>> foreignTableColumns = this.getTableDescription((String)foreignKey.get("foreignTableName"));
    		if (foreignTableColumns != null){
    			for (HashMap entry: foreignTableColumns){
    				HashMap<String, String> column = new HashMap<String,String>();
    				column.put("datatype", SqlDesignerTranslations.translateDatatype(((String)entry.get("data_type")).toUpperCase(), getFieldSize(entry))); 
    				foreignTableColumnMap.put((String)entry.get("column_name"), column);
    			}
    		}

    		response.addRelation(tableName, 
    				(String)foreignKey.get("constraintName"),
    				(String)foreignKey.get("columnName"), 
    				(String)foreignKey.get("foreignTableName"), 
    				(String)foreignKey.get("foreignColumnName"),
    				foreignTableColumnMap
    				);
    	}

    	// Add each index to the response
    	if (indexes.size() > 0) {
    		for (HashMap<String, Object> index : indexes) {
    			response.addIndex(tableName, 
    					(String) index.get("name"), 
    					(String) index.get("type"),
    					(List) index.get("columns"));
    		}
    	}  
    	return response;
    }

    public List<HashMap<String, String>> getForeignKeysFromPostgres(String table) throws SQLException {
        log.debug("getForeignKeysFromPostgres");
        
        String command = "SELECT " +
                "tc.constraint_name, tc.table_name, kcu.column_name, " +
                "ccu.table_name AS foreign_table_name, " +
                "ccu.column_name AS foreign_column_name " + 
            "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu " +
                    "ON tc.constraint_name = kcu.constraint_name " +
                "JOIN information_schema.constraint_column_usage ccu " +
                    "ON ccu.constraint_name = tc.constraint_name " +
            "WHERE "+
                "constraint_type = 'FOREIGN KEY' " +
                "AND tc.table_name = '" + table + "'";

        List foreignKeys = new ArrayList();
        HashMap<String, String> foreignKey;
        @SuppressWarnings("unchecked")
		List<Object[]> results = this.runSQLQuery(command, null, null, null);
        for ( Object[] row: results ) {
        	
                foreignKey = new HashMap();
                foreignKey.put("constraintName", row[0].toString());
                foreignKey.put("tableName", row[1].toString());
                foreignKey.put("columnName", row[2].toString());
                foreignKey.put("foreignTableName", row[3].toString());
                foreignKey.put("foreignColumnName", row[4].toString());
                foreignKeys.add(foreignKey);
            }

        return foreignKeys;
    }

	protected List<HashMap<String, Object>> getIndexesFromPostgres(String table) throws Exception {
        String command = "SELECT " +
            "i.relname as indexname, " +
            "idx.indrelid::regclass as tablename, " +
            "ARRAY( " +
                "SELECT pg_get_indexdef(idx.indexrelid, k + 1, true) " +
                "FROM generate_subscripts(idx.indkey, 1) as k " +
                "ORDER BY k " +
            ") as colnames, " +
            "indisunique as isunique, " +
            "indisprimary as isprimary " +
        "FROM " +   
            "pg_index as idx " +
            "JOIN pg_class as i " +
                "ON i.oid = idx.indexrelid " +
        "WHERE CAST(idx.indrelid::regclass as text) = quote_ident('"+table+"')";
		List<HashMap<String, Object>> indexes = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> index;
		String type;

		@SuppressWarnings("unchecked")
		List<Object[]> results = this.runSQLQuery(command, null, null, null);
		for ( Object[] row: results ) {
			
			index = new HashMap<String, Object>();
				index.put("name", row[0].toString());
				ArrayList<String> columns = new ArrayList<String>();
				columns.add(row[2].toString());
				index.put("columns", columns);
				String unique = row[3].toString();
				String primary = row[4].toString();
				if (primary.equals("t")) {
					type = "PRIMARY";
				}
				else if (unique.equals("t")) {
					type = "UNIQUE";
				}
				else {
					type = "INDEX";
				}
				index.put("type", type);

				indexes.add(index);
			
		}
		return indexes;
	}
	
	
	public List<HashMap<String, String>> getTableDescription(String table)
			throws Exception {
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

		String command = String
				.format("select %s from INFORMATION_SCHEMA.COLUMNS where table_name = %s ORDER BY ordinal_position ASC",
						StringUtils.join(fields.iterator(), ","),
						quote_ident(table));

		HashMap<String, String> columnDescription;
		List<HashMap<String, String>> columnDescriptions = new ArrayList<HashMap<String, String>>();
		@SuppressWarnings("unchecked")
		List<Object> rows = this.runSQLQuery(command, null, null, null);

		// First get all column names
		if (log.isDebugEnabled()) {
			log.debug(String.format("Found columns for table %s", table));
		}
		if (rows.size() != 1) {
			throw new Exception("Internal or programmer error");
		}
		Object[] row = (Object[]) rows.get(0);
		columnDescription = new HashMap<String, String>();
		int i = 0;
		for (String field : fields) {
			columnDescription.put(field, row[i++].toString());
		}

		columnDescriptions.add(columnDescription);

		if (log.isDebugEnabled()) {
			log.debug("getTableDescription:return " + columnDescriptions.size()
					+ " entries");
		}

		return columnDescriptions;
	}
	
	
    protected String getFieldSize(HashMap<String, String> column) {
        log.debug("getFieldSize");
		// Format the field size appropriately for the data type
		String fieldSize;
		if (column.get("data_type").compareToIgnoreCase("numeric") == 0) {
			fieldSize = String.format("%s,%s",
					column.get("numeric_precision"),
					column.get("numeric_scale"));
		} else {
			fieldSize = column.get("character_maximum_length");
		}
		return fieldSize;
    }


}
