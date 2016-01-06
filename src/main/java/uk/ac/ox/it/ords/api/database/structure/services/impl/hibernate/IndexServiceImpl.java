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
import javax.ws.rs.core.Response;

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
	public MessageEntity getIndex(int dbId, String instance, String tableName,
			String indexName, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
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
	public void createIndex(int dbId, String instance, String tableName,
			String indexName, IndexRequest newIndex, boolean staging) throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
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

        String query = "CREATE ? INDEX ? ON ? (?)";
        List<Object> parameters = this.createParameterList(unique, indexName, tableName, columns);
        this.runJDBCQuery(query, parameters, server, databaseName);
	}
	

	@Override
	public void updateIndex(int dbId, String instance, String tableName,
			String indexName, IndexRequest index, boolean staging)
			throws Exception {
        String newName = index.getNewname();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
       
        String query = "ALTER INDEX ? RENAME TO ?";
        List<Object> parameters = createParameterList ( indexName, newName);
        this.runJDBCQuery(query, parameters, server, databaseName);


	}

	@Override
	public void deleteIndex(int dbId, String instance, String tableName,
			String indexName, boolean staging) throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		this.runJDBCQuery("DROP index ?", createParameterList(indexName), server, databaseName);
	}

}