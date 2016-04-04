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

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang.StringUtils;

import uk.ac.ox.it.ords.api.database.structure.dto.IndexRequest;
import uk.ac.ox.it.ords.api.database.structure.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.IndexService;
import uk.ac.ox.it.ords.api.database.structure.services.MessageEntity;

public class IndexServiceImpl extends StructureServiceImpl
		implements
			IndexService {

	@Override
	public MessageEntity getIndex(OrdsPhysicalDatabase database, String tableName,
			String indexName, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkIndexExists(tableName, indexName, databaseName, server, userName, password)){
			throw new NotFoundException("");
		}
		return new MessageEntity(indexName);
	}

	@Override
	public void createIndex(OrdsPhysicalDatabase database, String tableName,
			String indexName, IndexRequest newIndex, boolean staging) throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
        Boolean isUnique = newIndex.isUnique();
        ArrayList columnsList = new ArrayList(); 
        // Check that the index has at least one column
        if (newIndex.getColumns() == null || newIndex.getColumns().isEmpty()) {
            //message = emd.getMessage("Rst035").getText();
            throw new BadParameterException("The index must have at least one column");
        }
        
        // Join the individual column names into a single string
        for (String column : newIndex.getColumns()) {
            columnsList.add(quote_ident(column));
        }
        String columns = StringUtils.join(columnsList.iterator(), ",");
        String unique = "";

        // Generate the SQL for creating the index
        if (isUnique != null && isUnique) {
            unique = "UNIQUE ";
        }

        String query = String.format("CREATE %sINDEX %s ON %s (%s)",
                unique,
                quote_ident(indexName),
                quote_ident(tableName),
                columns);
        //List<Object> parameters = this.createParameterList(unique, indexName, tableName, columns);
        this.runJDBCQuery(query, null, server, databaseName);
	}
	

	@Override
	public void updateIndex(OrdsPhysicalDatabase database, String tableName,
			String indexName, IndexRequest index, boolean staging)
			throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
        String newName = index.getNewname();
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		
		if ( !this.checkIndexExists(tableName, indexName, databaseName, server, userName, password)){
			throw new NotFoundException("");
		}
        if (indexName.equals(newName) || newName == null || newName.isEmpty()){
        	throw new BadParameterException();
        }
		String query = String.format("ALTER INDEX %s RENAME TO %s",
                quote_ident(indexName),
                quote_ident(newName));
        this.runJDBCQuery(query, null, server, databaseName);


	}

	@Override
	public void deleteIndex(OrdsPhysicalDatabase database, String tableName,
			String indexName, boolean staging) throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		String statement = String.format("DROP INDEX %s", quote_ident(indexName));
		
		this.runJDBCQuery(statement, null, server, databaseName);
	}

}
