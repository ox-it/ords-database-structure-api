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

import org.apache.commons.lang.StringUtils;

import uk.ac.ox.it.ords.api.database.structure.dto.ConstraintRequest;
import uk.ac.ox.it.ords.api.database.structure.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.structure.exceptions.NamingConflictException;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.ConstraintService;
import uk.ac.ox.it.ords.api.database.structure.services.MessageEntity;

public class ConstraintServiceImpl extends StructureServiceImpl
		implements
			ConstraintService {

	@Override
	public MessageEntity getConstraint(int dbId, String instance,
			String tableName, String constraintName, boolean staging)
			throws Exception {
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
		if (!this.checkConstraintExists(tableName, constraintName, databaseName, server, userName, password)){
			throw new NotFoundException();
		}
		return new MessageEntity(constraintName);
	}
	

	@Override
	public void createConstraint(int dbId, String instance, String tableName,
			String constraintName, ConstraintRequest newConstraint,
			boolean staging) throws Exception {
		String query = "";

		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		// message = String.format(emd.getMessage("Rst048").getText(),
		// constraintName);
		Boolean isUnique = newConstraint.isUnique();
		Boolean isForeign = newConstraint.isForeign();
		Boolean isPrimary = newConstraint.isPrimary();

		ArrayList columnsList = new ArrayList();
		ArrayList columnsArray = newConstraint.getColumns();
		String columns = "";

		if ((isUnique != null && isUnique) || (isForeign != null && isForeign)
				|| (isPrimary != null && isPrimary)) {
			// If we're creating a Unique, Foreign or Primary Key constaint
			if (columnsArray == null || columnsArray.isEmpty()) {
				// If no columns are specified, return an error
				// message = emd.getMessage("Rst035").getText();
				throw new BadParameterException("No columns specified");
			} else if ((isUnique != null && isUnique)
					|| (isPrimary != null && isPrimary)) {
				// If we're creating a Unique or Primary Key constraint,
				// join the columns into a string.
				for (String column : newConstraint.getColumns()) {
					columnsList.add(quote_ident(column));
				}
				columns = StringUtils.join(columnsList.iterator(), ",");
			} else {
				// If we're creating a foreign key, make sure there's
				// only one column
				if (columnsArray.size() > 1) {
					// message = emd.getMessage("Rst068").getText();
					throw new BadParameterException(
							"Only 1 column can be specified for a foreign key");
				}
			}
		}
		// Check that the specified table exists
		if (!this.checkTableExists(tableName, databaseName, server, userName, password)) {
			log.error(
					"Tried to create constraint %s for non-existant table %s",
					constraintName, tableName);
			// message = String.format(emd.getMessage("Rst052").getText(),
			// tableName);
			throw new NotFoundException();
		}

		// Get the next value from a special sequence created when the
		// database is first cloned in ORDS, which makes sure we can
		// create a unique name for the constraint
		String conIdQuery = "SELECT nextval('ords_constraint_seq'::regclass) AS id";
		String uniqueConstraintName = "";
		CachedRowSet result = this.runJDBCQuery(conIdQuery, null, server,
				databaseName);

		// Object result = this.singleResultQuery(conIdQuery, databaseName,
		// userName, password);
		if (result != null && result.size() > 0) {
			// Actually generate a name for the constraint
			result.first();
			int conId = result.getInt("id");
			uniqueConstraintName = String.format(constraintName + "_%d", conId);
		} else {
			uniqueConstraintName = constraintName;
		}

		// Generate the SQL for creating the constraint
		if (isUnique != null && isUnique) {
			query = String.format("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
                    quote_ident(tableName),
                    quote_ident(uniqueConstraintName),
                    columns);  
		} 
		else if (isPrimary != null && isPrimary) {
			query = String.format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
                    quote_ident(tableName),
                    quote_ident(uniqueConstraintName),
                    columns); 
		} 
		else if (isForeign != null && isForeign) {
			String column = newConstraint.getColumn();
			String refTable = newConstraint.getReftable();

			String refColumn = newConstraint.getRefcolumn();

			// If this is a foeign key, make sure there is a
			// referenced table specified
			if (refTable == null || refTable.isEmpty()) {
				// message = emd.getMessage("Rst049").getText();
				throw new BadParameterException(
						"A foreign key must have a reference table specified");
			}

			// Make sure there is a referenced column specified
			if (refColumn == null || refColumn.isEmpty()) {
				// message = emd.getMessage("Rst051").getText();
				throw new BadParameterException(
						"A reference column must be specified");
			}

			query = String.format("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) "
                    + "REFERENCES %s (%s)",
                        quote_ident(tableName),
                        quote_ident(uniqueConstraintName),
                        quote_ident(column),
                        quote_ident(refTable),
                        quote_ident(refColumn));
		} 
		else {
			// If this isn't a Unique, Foreign or Primary key constraint
			// make sure a check expression is defined and generate the
			// SQL. Check constraints currently aren't implemented in
			// the Schema Designer interface.
			String checkExpression = newConstraint.getCheckExpression();
			if (checkExpression == null || checkExpression.isEmpty()) {
				// message = emd.getMessage("Rst051").getText();
				throw new BadParameterException(
						"Check constraints are not supported");
			}

			query = String.format("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s) ",
                    quote_ident(tableName),
                    quote_ident(uniqueConstraintName),
                    checkExpression);
		}

		// Check that a constraint with this name doesn't already exist.
		if (this.checkConstraintExists(tableName, uniqueConstraintName,
				databaseName, server, userName, password)) {
			log.error(
					"Tried to create duplicate constraint name %s on table %s",
					uniqueConstraintName, tableName);
			// message = String.format(emd.getMessage("Rst053").getText(),
			// uniqueConstraintName,
			// tableName);
			throw new NamingConflictException(
					"Can't duplication constraint name");
		}

		// Create the constraint
		this.runJDBCQuery(query, null, server, databaseName);
	}

	

	@Override
	public void updateConstraint(int dbId, String instance, String tableName,
			String constraintName, ConstraintRequest constraint, boolean staging)
			throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		String newName = constraint.getNewname();
		String query = String.format("ALTER TABLE %s RENAME CONSTRAINT %s to %s", 
				quote_ident(tableName),
				quote_ident(constraintName),
				quote_ident(newName)
				);
		this.runJDBCQuery(query, null, server, databaseName);
	}

	
	@Override
	public void deleteConstraint(int dbId, String instance, String tableName,
			String constraintName, boolean staging) throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String databaseName = database.getDbConsumedName();
		if ( staging ) {
			databaseName = this.calculateStagingName(databaseName);
		}
		String server = database.getDatabaseServer();
		String query = String.format("ALTER TABLE %s DROP CONSTRAINT %s",
				quote_ident(tableName),
				quote_ident(constraintName)
		);
		this.runJDBCQuery(query, null, server, databaseName);
	}
	
	

}
