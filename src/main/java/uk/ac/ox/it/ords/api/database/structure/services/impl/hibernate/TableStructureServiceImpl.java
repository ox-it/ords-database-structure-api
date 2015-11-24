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


import javax.ws.rs.NotFoundException;

import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.Transaction;

import uk.ac.ox.it.ords.api.database.structure.exceptions.NamingConflictException;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;
import uk.ac.ox.it.ords.api.database.structure.services.TableStructureService;

public class TableStructureServiceImpl extends StructureServiceImpl implements TableStructureService {

	@Override
	public TableList getTableList(int dbId, String instance, boolean staging) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TableList getTableMetadata(int dbID, String instance,
			String tableName, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbID, instance, staging);
		if ( !this.checkTableExists(tableName, databaseName, userName, password)) {
			throw new NotFoundException(String.format("No table called %s found in database %s", tableName, databaseName));
		}
		TableList table = new TableList();
		String comment = this.tableComment(tableName, databaseName, userName, password);
		table.addTable(tableName, comment);
        addTableMetadata(tableName, table);
		return table;
	}

	@Override
	public void createNewTable(int dbID, String instance, String tableName, boolean staging)
			throws Exception {
        final String query = String.format("CREATE TABLE %s();", quote_ident(tableName));
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbID, instance, staging);
		if ( this.checkTableExists(tableName, databaseName, userName, password)) {
			throw new NamingConflictException(String.format("The table %s already exists in database %s", tableName, databaseName));
		}
		this.runSQLStatement(query, databaseName, userName, password);

	}

	@Override
	public void renameTable(int dbID, String instance, String tableName,
			String tableNewName, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbID, instance, staging);
		if ( !this.checkTableExists(tableName, databaseName, userName, password)) {
			throw new NotFoundException(String.format("No table called %s found in database %s", tableName, databaseName));
		}
		if ( this.checkTableExists(tableNewName, databaseName, userName, password)){
			throw new NamingConflictException("There is already a table called "+tableNewName+" in database "+databaseName);
		}
		String query = String.format("ALTER TABLE %s RENAME TO %s;", 
                quote_ident(tableName), 
                quote_ident(tableNewName));
		this.runSQLStatement(query, databaseName, userName, password);
        query = String.format("SELECT sequence_name FROM information_schema.sequences where sequence_name LIKE %s",
        		quote_ident(tableName+"%"));
        Session session = this.getOrdsDBSessionFactory().openSession();
        
        try {
        	Transaction transaction = session.beginTransaction();
        	SQLQuery sqlQuery = session.createSQLQuery(query);
        	String sequenceName = sqlQuery.uniqueResult().toString();
        	if (sequenceName != null ) {
        		
                String newSequenceName = sequenceName.replace(tableName, tableNewName);
                query = String.format("ALTER SEQUENCE %s RENAME TO %s",
                                        quote_ident(sequenceName),
                                        quote_ident(newSequenceName));
                SQLQuery sqlAlter = session.createSQLQuery(query);
                sqlAlter.executeUpdate();
            }
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

	@Override
	public void deleteTable(int dbID, String instance, String tableName, boolean staging)
			throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbID, instance, staging);
		if ( !this.checkTableExists(tableName, databaseName, userName, password)) {
			throw new NotFoundException(String.format("No table called %s found in database %s", tableName, databaseName));
		}
        final String query = String.format("DROP TABLE %s;", quote_ident(tableName));
        this.runSQLStatement(query, databaseName, userName, password);
        
	}

}
