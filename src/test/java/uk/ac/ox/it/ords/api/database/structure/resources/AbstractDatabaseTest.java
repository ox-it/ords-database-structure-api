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

package uk.ac.ox.it.ords.api.database.structure.resources;

import java.util.ArrayList;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.CommentRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.ConstraintRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.DatabaseRequest;
import uk.ac.ox.it.ords.api.database.structure.dto.IndexRequest;
import uk.ac.ox.it.ords.api.database.structure.resources.DatabaseTest.constraint_type;

public class AbstractDatabaseTest extends AbstractResourceTest{
	
	protected ColumnRequest buildColumnRequest ( String name, String dataType, String defaultValue, boolean nullable, boolean autoIncrement  ){
		ColumnRequest cr = new ColumnRequest();
		cr.setNewname(name);
		cr.setDatatype(dataType);
		cr.setDefaultvalue(defaultValue);
		cr.setNullable(nullable);
		cr.setAutoincrement(autoIncrement);
		return cr;
	}
	
	
	protected ConstraintRequest buildConstraintRequest( 
			String name, 
			constraint_type type, 
			String[] columnNames, 
			String refTable, 
			String refColumn) {
		ConstraintRequest cr = new ConstraintRequest();
		cr.setNewname(name);
		cr.setUnique(type == constraint_type.UNIQUE);
		cr.setPrimary(type == constraint_type.PRIMARY);
		cr.setForeign(type == constraint_type.FOREIGN);
		cr.setColumns(columnNames);
		cr.setReftable(refTable);
		cr.setRefcolumn(refColumn);
		return cr;
		
	}
	
	
	protected CommentRequest buildCommentRequest(
			String comment) {
		CommentRequest cr = new CommentRequest();
		cr.setComment(comment);
		return cr;
	}
	
	
	protected IndexRequest buildIndexRequest(
			String name,
			boolean unique,
			ArrayList<String> columns) {
		IndexRequest ir = new IndexRequest();
		ir.setNewname(name);
		ir.setUnique(unique);
		ir.setColumns(columns);
		return ir;
	}
	
	
	protected DatabaseRequest buildDatabaseRequest(String nullForNow, int projectDatabaseId, String serverName) {
		DatabaseRequest dbr = new DatabaseRequest();
		dbr.setDatabaseName(nullForNow);
		dbr.setDatabaseServer(serverName);
		dbr.setGroupId(projectDatabaseId);
		return dbr;
	}
	

}
