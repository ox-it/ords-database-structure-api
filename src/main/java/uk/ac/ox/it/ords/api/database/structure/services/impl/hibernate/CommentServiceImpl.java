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

import javax.ws.rs.NotFoundException;

import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.CommentService;

public class CommentServiceImpl extends StructureServiceImpl
		implements
			CommentService {

	@Override
	public String getTableComment(int dbId, String instance, String tableName,
			boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if (!this.checkTableExists(tableName, databaseName, server,userName, password)) {
			throw new NotFoundException();
		}
		return this.tableComment(databaseName, server, tableName);
	}

	@Override
	public void setTableComment(int dbId, String instance, String tableName,
			String comment, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if (!this.checkTableExists(tableName, databaseName, server, userName, password)) {
			throw new NotFoundException();
		}
		String statement = "COMMENT ON TABLE ? IS ?";
		List<Object> parameters = this.createParameterList(tableName, comment);
		this.runJDBCQuery(statement, parameters, server, databaseName);
	}

	@Override
	public String getColumnComment(int dbId, String instance, String tableName,
			String columnName, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkColumnExists(columnName, tableName, databaseName, server, userName, password)){
			throw new NotFoundException();
		}
		return this.columnComment(databaseName, server, tableName, columnName);
	}

	@Override
	public void setColumnComment(int dbId, String instance, String tableName,
			String columnName, String comment, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkColumnExists(columnName, tableName, databaseName, server, userName, password)){
			throw new NotFoundException();
		}
		this.runJDBCQuery("COMMENT ON COLUMN ? IS ?",
				createParameterList(tableName+"."+columnName, comment),
				server,
				databaseName);
	
	}

}
