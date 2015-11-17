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

package uk.ac.ox.it.ords.api.database.structure.services;

import java.util.HashMap;
import java.util.List;

public class TableList {

	private HashMap<String, HashMap<?, ?>> tables;

	public TableList() {
		this.tables = new HashMap();
	}

	public HashMap getTables() {
		return tables;
	}

	public void addTable(String tableName, String comment) {
		HashMap<String, Object> table = new HashMap();
		table.put("columns", new HashMap<String, HashMap>());
		table.put("relations", new HashMap<String, HashMap>());
		table.put("indexes", new HashMap<String, HashMap>());
		table.put("comment", comment);
		tables.put(tableName, table);
	}

	public void addColumn(String tableName, String name, int position,
			String defaultValue, boolean nullable, String datatype,
			boolean autoincrement, String comment) {
		HashMap columns = (HashMap) tables.get(tableName).get("columns");
		HashMap column = new HashMap();
		column.put("position", position);
		column.put("default", defaultValue);
		column.put("nullable", nullable);
		column.put("datatype", datatype);
		column.put("autoincrement", autoincrement);
		column.put("comment", comment);
		columns.put(name, column);
	}

	public void addIndex(String tableName, String indexName, String type,
			List<String> columns) {
		HashMap indexes = (HashMap) tables.get(tableName).get("indexes");
		HashMap<String, Object> index = new HashMap();
		index.put("type", type);
		index.put("columns", columns);
		indexes.put(indexName, index);
	}

	public void addRelation(String tableName, String constraintName,
			String columnName, String referenceTable, String referenceColumn,
			HashMap<String, HashMap<String, String>> foreignTableColumns) {
		HashMap relations = (HashMap) tables.get(tableName).get("relations");
		HashMap<String, Object> relation = new HashMap();
		relation.put("column", columnName);
		relation.put("referenceTable", referenceTable);
		relation.put("referenceColumn", referenceColumn);
		relation.put("columns", foreignTableColumns);
		relations.put(constraintName, relation);
	}

	public void setXY(String tableName, int x, int y) {
		HashMap table = tables.get(tableName);
		table.put("x", x);
		table.put("y", y);
	}

}
