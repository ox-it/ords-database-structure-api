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

import uk.ac.ox.it.ords.api.database.structure.services.CommentService;

public class CommentServiceImpl extends StructureServiceImpl
		implements
			CommentService {

	@Override
	public String getTableComment(int dbId, String instance, String tableName,
			boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbId, instance, staging);
		if (!this.checkTableExists(tableName, databaseName, userName, password)) {
			throw new NotFoundException();
		}
		return this.getTableComment(dbId, instance, tableName, staging);
	}

	@Override
	public void setTableComment(int dbId, String instance, String tableName,
			String comment, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbId, instance, staging);
		if (!this.checkTableExists(tableName, databaseName, userName, password)) {
			throw new NotFoundException();
		}
		String statement = String.format("COMMENT ON TABLE %s IS %s",
				quote_ident(tableName),
				quote_literal(comment));
		this.runSQLStatement(statement, databaseName, userName, password);
	}

	@Override
	public String getColumnComment(int dbId, String instance, String tableName,
			String columnName, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbId, instance, staging);
		if ( !this.checkColumnExists(columnName, tableName, databaseName, userName, password)){
			throw new NotFoundException();
		}
		return this.getColumnComment(dbId, instance, tableName, columnName, staging);
	}

	@Override
	public void setColumnComment(int dbId, String instance, String tableName,
			String columnName, String comment, boolean staging) throws Exception {
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbId, instance, staging);
		if ( !this.checkColumnExists(columnName, tableName, databaseName, userName, password)){
			throw new NotFoundException();
		}
		String identifier = String.format("%s.%s", 
                quote_ident(tableName), 
                quote_ident(columnName));
		String statement = String.format("COMMENT ON COLUMN %s IS %s",
				identifier,
				quote_literal(comment));
		this.runSQLStatement(statement, databaseName, userName, password);
	
	}

}
