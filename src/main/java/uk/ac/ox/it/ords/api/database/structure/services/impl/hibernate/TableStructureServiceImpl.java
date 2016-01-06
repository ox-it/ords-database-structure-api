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

import javax.ws.rs.NotFoundException;

import org.hibernate.Criteria;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;

import uk.ac.ox.it.ords.api.database.structure.dto.PositionRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.TablePosition;
import uk.ac.ox.it.ords.api.database.structure.exceptions.NamingConflictException;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.model.SchemaDesignerTable;
import uk.ac.ox.it.ords.api.database.structure.services.TableList;
import uk.ac.ox.it.ords.api.database.structure.services.TableStructureService;

public class TableStructureServiceImpl extends StructureServiceImpl implements TableStructureService {

	@Override
	public TableList getTableList(int dbId, String instance, boolean staging) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TableList getTableMetadata(int dbId, String instance,
			String tableName, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkTableExists(tableName, databaseName,server, userName, password)) {
			throw new NotFoundException(String.format("No table called %s found in database %s", tableName, databaseName));
		}
		TableList table = new TableList();
		String comment = this.tableComment(databaseName, server, tableName);
		table.addTable(tableName, comment);
        addTableMetadata(databaseName, server, tableName, table);
		return table;
	}

	@Override
	public void createNewTable(int dbId, String instance, String tableName, boolean staging)
			throws Exception {
 		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( this.checkTableExists(tableName, databaseName, server, userName, password)) {
			throw new NamingConflictException(String.format("The table %s already exists in database %s", tableName, databaseName));
		}
		// aargh prepared statements don't work with create table so we have to format the string ourselves!
		String statement = "CREATE TABLE \""+tableName+"\"();";
		this.runJDBCQuery(statement, null, server, databaseName);

	}

	@Override
	public void renameTable(int dbId, String instance, String tableName,
			String tableNewName, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkTableExists(tableName, databaseName, server, userName, password)) {
			throw new NotFoundException(String.format("No table called %s found in database %s", tableName, databaseName));
		}
		if ( this.checkTableExists(tableNewName, databaseName,server, userName, password)){
			throw new NamingConflictException("There is already a table called "+tableNewName+" in database "+databaseName);
		}
		String query = "ALTER TABLE ? RENAME TO ?;";
		List<Object> parameters = createParameterList(tableName, tableNewName );
		this.runJDBCQuery(query, parameters, server, databaseName);

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
	public void deleteTable(int dbId, String instance, String tableName, boolean staging)
			throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkTableExists(tableName, databaseName, server, userName, password)) {
			throw new NotFoundException(String.format("No table called %s found in database %s", tableName, databaseName));
		}
		this.runJDBCQuery("DROP TABLE ?", createParameterList(tableName), server, databaseName);
	}

	@Override
	public void setTablePositions(int dbId, String instance,
			PositionRequest positionRequest) throws Exception {
        List<String> tableNames = new ArrayList<String>();
        // Create or update tables from the schema designer
        for (TablePosition tablePosition : positionRequest.getPositions()) {
            // If there's no saved position for the table, create one.
            // otherwise, update the existing one.
            tableNames.add(tablePosition.getTablename());
            SchemaDesignerTable table = this.getTablePositionRecord(dbId, tablePosition.getTablename());
            if (table == null) {
                table = new SchemaDesignerTable();
                table.setDatabaseId(dbId);
                table.setTableName(tablePosition.getTablename());
                table.setX(tablePosition.getX());
                table.setY(tablePosition.getY());
                this.saveModelObject(table);
            } else {
                table.setX(tablePosition.getX());
                table.setY(tablePosition.getY());
                this.updateModelObject(table);
            }
        }
        // Remove any data for tables no longer in the schema
        List<SchemaDesignerTable> savedTables = this.getTablePositionRecordsForDatabase(dbId);
        for (SchemaDesignerTable savedTable : savedTables) {
            if (!tableNames.contains(savedTable.getTableName())) {
                this.removeModelObject(savedTable);
            }    
        }
		
		
	}
	
	
	private SchemaDesignerTable getTablePositionRecord ( int dbId, String tableName ) {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			Criteria criteria = session.createCriteria(SchemaDesignerTable.class);
			criteria.add(Restrictions.eq("databaseId", dbId));
			criteria.add(Restrictions.eq("tableName", tableName));
			@SuppressWarnings("unchecked")
			List<SchemaDesignerTable> tables =  (List<SchemaDesignerTable>)criteria.list();
			transaction.commit();
			if ( tables.size() == 1) {
				return tables.get(0);
			}
			return null;
		} 
		catch (Exception e) {
			session.getTransaction().rollback();
			throw e;
		} 
		finally {
			session.close();
		}
	}
	
	
	private List<SchemaDesignerTable>	getTablePositionRecordsForDatabase ( int dbId ) {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			Criteria criteria = session.createCriteria(SchemaDesignerTable.class);
			criteria.add(Restrictions.eq("databaseId", dbId));
			List<SchemaDesignerTable> tables = (List<SchemaDesignerTable>)criteria.list();
			transaction.commit();
			return tables;
		}
		catch (Exception e) {
			session.getTransaction().rollback();
			throw e;
		} 
		finally {
			session.close();
		}
	}
	
	
	

}
