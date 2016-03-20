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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissions;
import uk.ac.ox.it.ords.api.database.structure.services.OdbcService;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;

public class OdbcServiceImpl extends StructureServiceImpl implements OdbcService {

	private static Logger log = Logger.getLogger(OdbcServiceImpl.class);
	
    public static final String SCHEMA_NAME = "public";
	
	@Override
	public void addOdbcUserToDatabase(String role, String odbcPassword, OrdsPhysicalDatabase database, String databaseName) throws Exception{
		
		createNewRole(role, odbcPassword, database, databaseName);
		if (SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_MODIFY(database.getPhysicalDatabaseId())
			)){
			this.provideWriteAccessToDB(role, odbcPassword, database, databaseName);
			//TODO audit
		}
		if (SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_VIEW(database.getPhysicalDatabaseId())
			)){
			this.provideReadAccessToDB(role, odbcPassword, database, databaseName);
			//TODO audit
		}
	}

	@Override
	public void removeOdbcUserFromDatabase(String role, OrdsPhysicalDatabase database, String databaseName) throws Exception {
		// TODO audit
		this.revokeFromDatabase(role, database, databaseName);

	}
	
	@Override
    public void resetOdbcRolePassword(String roleName, String newPassword, OrdsPhysicalDatabase database, String databaseName) throws Exception {
                
        if ( (roleName == null) || (roleName.isEmpty()) ) {
        	throw new Exception("No role provided");
        }
        else if ( (newPassword == null) || (newPassword.isEmpty()) ) {
        	throw new Exception("No password provided");
        }
        else {
			String command = String.format("alter user \"%s\" password '%s'",
					roleName,
			        newPassword);
			this.runSQLStatement(command, database.getDatabaseServer(), databaseName);
        }
        // TODO Audit
    }
	
    /**
     * Create a role in ODBC.
     * 
     * @param roleName the rolename of the user - this maps to their email address
     * @param userPassword the role's password to be used
     * @return true if successful
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     */
    private void createNewRole(String roleName, String userPassword, OrdsPhysicalDatabase database, String databaseName) throws Exception {
        log.debug("createNewRole for ODBC");
                
        if ( (roleName == null) || (roleName.isEmpty()) ) {
        	throw new Exception("no role name provided");
        }
        else {
        	if (doesRoleExist(roleName, database, databaseName)) {
        		log.info("Role already exists - no need to create this");
        	} else {
        		/*
        		 * In creating the command to create a user, some of the defaults are specified explicitly
        		 * for clarity and future proofing.
        		 * The documentation states that if no "valid until" clause is specified in the create role command, the role is 
        		 * valid indefinitely. In practise I have not found this to be the case, so here I specify the valid until
        		 * field explicitly.
        		 */
        		String command = String.format("create role \"%s\" nosuperuser login createdb inherit nocreaterole password '%s' valid until '2045-01-01'",
        				roleName,
        				userPassword);
        		try {
        			System.out.println(command);
        			runSQLStatement(command, database.getDatabaseServer(), databaseName);
        		} catch (Exception e) {
        			throw(e);
        		}
        	}
        }

    }
    
    
    /**
     * Find role on server
     * @param roleName the role to search
     * @return true if that role exists
     */
    private boolean doesRoleExist(String roleName, OrdsPhysicalDatabase database, String databaseName) throws Exception { 
        String query = String.format("SELECT 1 FROM pg_roles WHERE rolname='%s'", roleName);
        System.out.println(database.getDatabaseServer());
        System.out.println(databaseName);
        System.out.println(query);
        CachedRowSet result = runJDBCQuery(query, null, database.getDatabaseServer(), databaseName);
        System.out.println(result);
        boolean ret = result.first();
        System.out.println(ret);
        return ret;
    }
    
	private boolean provideWriteAccessToDB(String odbcName, String odbcPassword, OrdsPhysicalDatabase database, String databaseName) throws Exception {
        return provideAccess(odbcName, odbcPassword, database, databaseName, true );
    }
    
    private boolean provideReadAccessToDB(String odbcName, String odbcPassword, OrdsPhysicalDatabase database, String databaseName) throws Exception {
        return provideAccess(odbcName, odbcPassword, database, databaseName, false);
    }
    
    
    /**
     * Provide ODBC access to a database for a specific user. This function will first remove their
     * access rights and then re-add them.
     * @param odbcName
     * @param dbName
     * @param write
     * @param actor
     * @param actorPassword
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws DBEnvironmentException
     */
    private boolean provideAccess(String odbcName, String odbcPassword, OrdsPhysicalDatabase database,  String databaseName, boolean write) throws Exception {
    	
    	// Check if the role exists on the server, and create if not
    	if (doesRoleExist(odbcName, database, databaseName)) {    	
	    	// First revoke access. This is just to make sure things are clean before we
            // continue.
	    	revokeFromDatabase(odbcName, database, databaseName);
    	}
    	else {
    		createNewRole(odbcName, odbcPassword, database, databaseName);
    	}
    	
    	
    	List<String> commandList = getAccessStatements(odbcName, database, databaseName, write);
    	this.runSQLStatements(commandList, database.getDatabaseServer(), databaseName);

    	// Some commands can only be run as a Postgres admin - there is one command in this work package like that, so run that here
    	String s = getSpecialAccessStatements(odbcName, write);
    	this.runSQLStatement(s, database.getDatabaseServer(), databaseName);
    	return true;
    }
    
    private List<String> getAccessStatements(String roleName, OrdsPhysicalDatabase database, String databaseName, boolean write) throws ClassNotFoundException, SQLException {
        
        List<String> commandList = new ArrayList<String>();
        commandList.add(String.format("grant connect on database \"%s\" to \"%s\"",
                    databaseName,
                    roleName));
        List<String> otherCommands;
        if (write) {
            log.debug("Write access requested for user");
            otherCommands = getWriteAccessGrantStatement(roleName, database, databaseName);
        }
        else {
            log.debug("Read access requested");
            otherCommands = getReadAccessGrantStatement(roleName, database, databaseName);
        }
        
        commandList.addAll(otherCommands);

        return commandList;
    }
    
    public static String getSpecialAccessStatements(String roleName, boolean write) {
    	if (write) {
    		return String.format("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON TABLES TO \"%s\";", SCHEMA_NAME, roleName);
    	}
    	else {
    		return String.format("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT ON TABLES TO \"%s\";", SCHEMA_NAME, roleName);
    	}
    }
	
    private boolean revokeFromDatabase(String odbcNameToRevoke, OrdsPhysicalDatabase database, String databaseName) throws Exception {

    	List<String> commandList = getRevokeStatement(odbcNameToRevoke, database, databaseName);
    	runSQLStatements(commandList, database.getDatabaseServer(), databaseName);
    	// Some commands can only be run as a Postgres admin - there is one command in this work package like that, so run that here
    	String s = getSpecialRevokeStatement(odbcNameToRevoke);
    	runSQLStatement(s, database.getDatabaseServer(), databaseName);
    	return true;
    }
    
    //Need to determine what to revoke with regards to the schema authorisation - not enough to just revoke access to the db
    private List<String> getRevokeStatement(String roleName, OrdsPhysicalDatabase database, String databaseName) throws ClassNotFoundException, SQLException {


    	List<String> commandList = new ArrayList<String>(); 

    	commandList.add(String.format("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA \"%s\" FROM \"%s\";", SCHEMA_NAME, roleName));
    	commandList.add(String.format("revoke all on schema %s from \"%s\";", SCHEMA_NAME, roleName));
    	commandList.add(String.format("revoke connect on database \"%s\" from \"%s\";", databaseName, roleName));
    	return commandList;
    }
    
    protected List<String> getReadAccessGrantStatement(String roleName, OrdsPhysicalDatabase database, String databaseName) throws ClassNotFoundException, SQLException {
    	List<String> commandList = new ArrayList<String>();
    	commandList.add(String.format("GRANT usage ON SCHEMA %s TO \"%s\";", SCHEMA_NAME, roleName));
    	commandList.add(String.format("GRANT select ON all tables in schema %s TO \"%s\";", SCHEMA_NAME, roleName));
    	return commandList;
    }

    protected List<String> getWriteAccessGrantStatement(String roleName, OrdsPhysicalDatabase database, String dbName) throws ClassNotFoundException, SQLException {
        List<String> commandList = new ArrayList<>();
        commandList.add(String.format("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO \"%s\";", SCHEMA_NAME, roleName));
        commandList.add(String.format("GRANT usage, create ON SCHEMA %s TO \"%s\";", SCHEMA_NAME, roleName));
        return commandList;
    }
    
    protected String getSpecialRevokeStatement(String roleName) {
    	return String.format("alter default privileges in schema %s revoke all on tables from \"%s\" ;", SCHEMA_NAME, roleName);
    }
    
    //
    // Utility Methods
    //
    
	protected void runSQLStatement(String statement, String server, String databaseName) throws Exception{
		ArrayList<String> statements = new ArrayList<String>();
		statements.add(statement);
		runSQLStatements(statements, server, databaseName);
	}
	
	protected void runSQLStatements(List<String> statements, String server,
			String databaseName) throws Exception {
		Connection connection = null;
		Properties connectionProperties = new Properties();
		PreparedStatement preparedStatement = null;
			Configuration config = MetaConfiguration.getConfiguration();
			connectionProperties.put("user", config.getString(StructureServiceImpl.ORDS_DATABASE_USER));
			connectionProperties.put("password", config.getString(StructureServiceImpl.ORDS_DATABASE_PASSWORD));
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
}