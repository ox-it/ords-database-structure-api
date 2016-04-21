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

import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.CommentService;

public class CommentServiceImpl extends StructureServiceImpl
		implements
			CommentService {

	@Override
	public String getTableComment(OrdsPhysicalDatabase database, String tableName,
			boolean staging) throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if (!this.checkTableExists(tableName, databaseName, server)) {
			throw new NotFoundException();
		}
		return this.tableComment(databaseName, server, tableName);
	}

	@Override
	public void setTableComment(OrdsPhysicalDatabase database, String tableName,
			String comment, boolean staging) throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if (!this.checkTableExists(tableName, databaseName, server)) {
			throw new NotFoundException();
		}
		String statement = "COMMENT ON TABLE %s IS %s";
		statement = String.format(statement, quote_ident(tableName), quote_literal(comment));
		this.runJDBCQuery(statement, null, server, databaseName);
	}

	@Override
	public String getColumnComment(OrdsPhysicalDatabase database, String tableName,
			String columnName, boolean staging) throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkColumnExists(columnName, tableName, databaseName, server)){
			throw new NotFoundException();
		}
		return this.columnComment(databaseName, server, tableName, columnName);
	}

	@Override
	public void setColumnComment(OrdsPhysicalDatabase database, String tableName,
			String columnName, String comment, boolean staging) throws Exception {
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		if ( !this.checkColumnExists(columnName, tableName, databaseName, server)){
			throw new NotFoundException();
		}
		String statement = "COMMENT ON COLUMN %s IS %s";
		String identifier = quote_ident(tableName)+"."+quote_ident(columnName);
		statement = String.format(statement, identifier, quote_literal(comment));
		this.runJDBCQuery(statement,
				null,
				server,
				databaseName);
	
	}

}
