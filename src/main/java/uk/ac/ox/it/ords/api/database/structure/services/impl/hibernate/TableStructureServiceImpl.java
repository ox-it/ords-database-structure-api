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

import org.hibernate.Criteria;
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
	public TableList getTableMetadata(OrdsPhysicalDatabase database,
			String tableName, boolean staging) throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkTableExists(tableName, databaseName,server)) {
			throw new NotFoundException(String.format("No table called %s found in database %s", tableName, databaseName));
		}
		TableList table = new TableList();
		String comment = this.tableComment(databaseName, server, tableName);
		table.addTable(tableName, comment);
        addTableMetadata(databaseName, server, tableName, table);
		return table;
	}

	@Override
	public void createNewTable(OrdsPhysicalDatabase database, String tableName, boolean staging)
			throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( this.checkTableExists(tableName, databaseName, server)) {
			throw new NamingConflictException(String.format("The table %s already exists in database %s", tableName, databaseName));
		}
		// aargh prepared statements don't work with create table so we have to format the string ourselves!
		String statement = "CREATE TABLE \""+tableName+"\"();";
		this.runJDBCQuery(statement, null, server, databaseName);

	}

	@Override
	public void renameTable(OrdsPhysicalDatabase database, String tableName,
			String tableNewName, boolean staging) throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkTableExists(tableName, databaseName, server)) {
			throw new NotFoundException(String.format("No table called %s found in database %s", tableName, databaseName));
		}
		if ( this.checkTableExists(tableNewName, databaseName,server)){
			throw new NamingConflictException("There is already a table called "+tableNewName+" in database "+databaseName);
		}
		String query = String.format("ALTER TABLE %s RENAME TO %s;", quote_ident(tableName), quote_ident(tableNewName));
		this.runJDBCQuery(query, null, server, databaseName);

        query = "SELECT sequence_name FROM information_schema.sequences where sequence_name LIKE ?";
        List<Object> parameters = this.createParameterList(tableName+"%");
        CachedRowSet result = this.runJDBCQuery(query, parameters, server, databaseName);
        
        if ( result.first() ) {
        	String sequenceName = result.getString("sequence_name");
        	String newSequenceName = sequenceName.replace(tableName, tableNewName);
        	query = String.format("ALTER SEQUENCE %s RENAME TO %s",
                    quote_ident(sequenceName),
                    quote_ident(newSequenceName));
        	this.runJDBCQuery(query, null, server, databaseName);
        }
	}

	@Override
	public void deleteTable(OrdsPhysicalDatabase database, String tableName, boolean staging)
			throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkTableExists(tableName, databaseName, server)) {
			throw new NotFoundException(String.format("No table called %s found in database %s", tableName, databaseName));
		}
		this.runJDBCQuery(String.format("DROP TABLE %s", tableName), null, server, databaseName);
	}
	
	

	@Override
	public void setTablePositions(OrdsPhysicalDatabase database,
			PositionRequest positionRequest) throws Exception {
        List<String> tableNames = new ArrayList<String>();
        // Create or update tables from the schema designer
        for (TablePosition tablePosition : positionRequest.getPositions()) {
            // If there's no saved position for the table, create one.
            // otherwise, update the existing one.
            tableNames.add(tablePosition.getTablename());
            SchemaDesignerTable table = this.getTablePositionRecord(database.getPhysicalDatabaseId(), tablePosition.getTablename());
            if (table == null) {
                table = new SchemaDesignerTable();
                table.setDatabaseId(database.getPhysicalDatabaseId());
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
        List<SchemaDesignerTable> savedTables = this.getTablePositionRecordsForDatabase(database.getPhysicalDatabaseId());
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
