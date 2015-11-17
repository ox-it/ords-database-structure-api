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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import javax.ws.rs.NotFoundException;

import org.hibernate.Transaction;
import org.apache.commons.lang.StringUtils;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.type.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.structure.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.structure.metadata.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.services.ColumnStructureService;

public class ColumnStructureServiceImpl extends StructureServiceImpl
		implements
			ColumnStructureService {
	private static Logger log = LoggerFactory
			.getLogger(ColumnStructureServiceImpl.class);

	/**
	 * A list of types that are some sort of Integer in postgres
	 */
	private final List<String> integerTypes = Arrays.asList("int", "integer",
			"bigint", "smallint", "int2", "int4", "int8");


	public ColumnRequest getColumnMetadata(int dbId, String instance,
			String tableName, String columnName, boolean staging) throws Exception {

		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbId, instance, staging);
		Session session = this.getUserDBSessionFactory(databaseName, userName,
				password).getCurrentSession();

		ArrayList<String> fields = new ArrayList<String>();
		fields.add("column_name");
		fields.add("data_type");
		// fields.add("character_maximum_length");
		// fields.add("numeric_precision");
		// fields.add("numeric_scale");
		fields.add("column_default");
		fields.add("is_nullable");
		// fields.add("ordinal_position");

		String command = String
				.format("select %s from INFORMATION_SCHEMA.COLUMNS where table_name = :table and column_name = :column ORDER BY ordinal_position ASC",
						StringUtils.join(fields.iterator(), ","));

		SQLQuery query = session.createSQLQuery(command);
		query.setParameter("table", tableName);
		query.setParameter("column", columnName);
		query.addScalar("column_name", new StringType());
		query.addScalar("data_type", new StringType());
		query.addScalar("column_default", new StringType());
		query.addScalar("is_nullable", new StringType());

		@SuppressWarnings("rawtypes")
		List results = query.list();
		if (results.size() != 1) {
			return null;
		}
		@SuppressWarnings("rawtypes")
		Map row = (Map) results.get(0);

		ColumnRequest column = new ColumnRequest();

		column.setNewname(row.get("column_name").toString());
		column.setDatatype(row.get("data_type").toString());
		column.setDefaultvalue(row.get("column_default").toString());
		boolean nullable = row.get("is_nullable").toString()
				.compareToIgnoreCase("YES") == 0;
		column.setNullable(nullable);

		return column;
	}

	public void createColumn(int dbId, String instance, String tableName,
			String columnName, ColumnRequest column,boolean staging) throws Exception {

		String datatype = column.getDatatype();
		String sequenceName = generateSequenceName(tableName, columnName);
		Boolean nullable = column.isNullable() != null && column.isNullable();
		Boolean autoinc = column.isAutoincrement() != null
				&& column.isAutoincrement();
		Boolean hasDefault = column.getDefaultvalue() != null
				&& !column.getDefaultvalue().isEmpty();
		if (datatype == null || datatype.isEmpty()) {
			log.error(String.format("Data type for new column %s was empty",
					columnName));
			throw new BadParameterException(
					"Data type for column cannot be empty");
		}
		datatype = SqlDesignerTranslations.convertDatatypeForPostgres(datatype);

		// Determine whether or not the field can be NULL
		String nullConstraint = "NOT NULL";
		if (nullable) {
			nullConstraint = "NULL";
		}
		// Check that the specified default is compatible with the other
		// metadata. If the field is not nullable, a default must be set,
		// unless it's auto-incramented in which case it is automatically
		// set and shouldn't be specified manually. If the field is nullable.
		// the default will be automatically set to null.
		String defaultConstraint;
		if (hasDefault) {
			if (autoinc) {
				log.error(String
						.format("Attempted to create sequence for column %s with default value %s",
								columnName, datatype));
				throw new BadParameterException(
						"Auto increment columns must not have a default value");
			}
			defaultConstraint = String.format(" DEFAULT '%s'",
					column.getDefaultvalue());
		} else {
			if (nullable) {
				defaultConstraint = " DEFAULT null";
			} else if (autoinc) {
				defaultConstraint = String.format(" DEFAULT nextval('%s')",
						generateSequenceName(tableName, columnName));
			} else {
				log.error("Attempt to create a not null field without a default value");
				throw new BadParameterException(
						"Field specified as NOT NULL must have a non-null default value");
			}
		}

		// If the column is auto-incremented, check that it's an integer
		// type

		if (autoinc) {
			if (!integerTypes.contains(datatype)) {
				log.error(String
						.format("Attempted to create sequence for column %s with datatype %s",
								columnName, datatype));
				throw new BadParameterException(
						"Only integer fields can have auto-increment enabled");
			}
		}

		// start the transaction
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();
		String databaseName = this.dbNameFromIDInstance(dbId, instance, staging);
		Session session = this.getUserDBSessionFactory(databaseName, userName,
				password).getCurrentSession();
		try {
			Transaction tx = session.beginTransaction();

			String query;
			// do all the stuff
			String constraints = nullConstraint + defaultConstraint;
			query = String.format("ALTER TABLE %s ADD COLUMN %s %s %s;",
					quote_ident(tableName), quote_ident(columnName), datatype,
					constraints);
			session.createSQLQuery(query).executeUpdate();
			if (autoinc) {
				query = String.format("CREATE SEQUENCE %s", sequenceName);
				session.createSQLQuery(query).executeUpdate();
				query = String.format("ALTER SEQUENCE %s OWNED BY %s.%s",
						sequenceName, tableName, columnName);
				session.createSQLQuery(query).executeUpdate();
			}
			tx.commit();
		} finally {
			session.close();
		}
	}


	public void updateColumn(int dbId, String instance, String tableName,
			String columnName, ColumnRequest request, boolean staging) throws Exception {
		String databaseName = this.dbNameFromIDInstance(dbId, instance, staging);
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();

		if (!this.checkColumnExists(columnName, tableName, databaseName,
				userName, password)) {
			throw new NotFoundException(
					String.format("Column name %s does not exist",columnName));
		}
		log.debug("doQuery");
		String newName = request.getNewname();
		Boolean nullable = request.isNullable();
		String datatype = request.getDatatype();
		String defaultValue = request.getDefaultvalue();
		Boolean autoinc = request.isAutoincrement();
		String sequenceName = "";
		String query;
		String message = "";

		// Check that some new metadata has been specified
		if ((newName == null || newName.isEmpty()) && nullable == null
				&& (datatype == null || datatype.isEmpty())
				&& defaultValue == null && autoinc == null) {
			log.error("Null values set in column request");
			throw new BadParameterException("Null values in column request");
		}

		if (autoinc != null) {
			// If the auto-incrament status is being changed
			if (autoinc) {
				// If we're "enabling" autoincrement, check that a
				// default value hasn't been set as well.
				if (defaultValue != null && !defaultValue.isEmpty()) {
					log.error("Specified a default for an auto-increment field");
					throw new BadParameterException(
							"You cannot specify a default for an auto-increment field");
				}
				// Get the data type and any current sequence for the
				// column.
				String command = String
						.format("SELECT data_type, pg_get_serial_sequence(%s, %s) AS sequence"
								+ " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = %s"
								+ " AND column_name = %s", 
								quote_ident(tableName),
								quote_ident(columnName),
								quote_ident(tableName),
								quote_ident(columnName));
				@SuppressWarnings("rawtypes")
				List rows = this.runSQLQuery(command, databaseName, userName,
						password);
				Object[] cols = (Object[]) rows.get(0);
				String currentDatatype = cols[0].toString();
				sequenceName = cols[1].toString();
				if (sequenceName != null && !sequenceName.isEmpty()) {
					message = String
							.format("Cannot add auto-increment to field %s: it is already auto-incremented",
									columnName);
					log.error(message);
					throw new BadParameterException(message);
				}
				if (!integerTypes.contains(currentDatatype.toLowerCase())
						&& (datatype == null || datatype.isEmpty() || !integerTypes
								.contains(datatype.toLowerCase()))) {
					if (datatype != null && !integerTypes.contains(datatype)) {
						message = String
								.format("Attempted to create sequence for column %s with datatype %s",
										columnName, datatype);
					} else {
						message = String
								.format("Attempted to create sequence for column %s with datatype %s",
										columnName, currentDatatype);
					}
					throw new BadParameterException(
							"Only integer fields can have auto-increment enabled");
				}
			} else {
				// If we're "disabling" autoincrement, check that there's
				// an existing sequence to remove.
				String command = String.format(
						"SELECT pg_get_serial_sequence(%s, %s) AS sequence",
						quote_ident(tableName), quote_ident(columnName));
				@SuppressWarnings("rawtypes")
				List rows = this.runSQLQuery(command, databaseName, userName,
						password);
				Object[] cols = (Object[]) rows.get(0);
				sequenceName = cols[0].toString();
				if (sequenceName.isEmpty()) {
					log.error("Attempt to remove autoincrement where non is set");
					throw new BadParameterException(
							"Auto-increment is not set so cannot be removed");
				}
			}
		}
		// If a new name for the column is specified, check that a column
		// with that name doesn't already exist in the table
		if (newName != null
				&& !newName.isEmpty()
				&& checkColumnExists(newName, tableName, databaseName,
						userName, password)) {

			log.error(
					"Attempted to rename column %s to existing name %s in table %s",
					columnName, newName, tableName);
			message = String
					.format("Cannot rename field to %s: field with that name already exists in table %s",
							newName, tableName);
			throw new BadParameterException(message);
		}
		// Validation all done, now perform the specified operations
		// If we're changing the nullability, create an execute the
		// approprate ALTER TABLE query
		if (nullable != null) {
			String operation;
			if (nullable) {
				operation = "DROP";
				message += String.format("Field %s no longer nullable",
						columnName) + "\n";
			} else {
				operation = "SET";
				message += String.format("Field %s now nullable", columnName)
						+ "\n";
			}
			query = String.format("ALTER TABLE %s ALTER %s %s NOT NULL;",
					quote_ident(tableName), quote_ident(columnName), operation);
			ArrayList<String> list = new ArrayList<String>();
			list.add(query);
			this.runSQLStatements(list, databaseName, userName, password);
		}
		ArrayList<String> statements = new ArrayList<String>();
		if (datatype != null && !datatype.isEmpty()) {
			// If the data type is being altered get the existing data type
			String convertedCol = quote_ident(columnName);
			String command = String.format("SELECT data_type"
					+ " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = %s"
					+ " AND column_name = %s", quote_ident(tableName),
					quote_ident(columnName));
			@SuppressWarnings("rawtypes")
			List rows = this.runSQLQuery(command, databaseName, userName,
					password);
			Object[] cols = (Object[]) rows.get(0);
			String fromDataType = cols[0].toString();
			if (fromDataType.equals("integer") || fromDataType.equals("bigint")) {
				if (datatype.toLowerCase().contains("date")
						|| datatype.toLowerCase().contains("time")) {
					convertedCol = String.format("to_timestamp(%s)",
							quote_ident(columnName));
				}
			} else if (fromDataType.contains("date")
					|| fromDataType.contains("time")) {
				if (datatype.toLowerCase().equals("integer")
						|| datatype.toLowerCase().equals("bigint")) {
					convertedCol = String.format("extract(epoch from %s)",
							quote_ident(columnName));
				}
			}
			datatype = SqlDesignerTranslations
					.convertDatatypeForPostgres(request.getDatatype());
			query = String
					.format("ALTER TABLE %1$s ALTER %2$s TYPE %3$s USING CAST(%4$s AS %3$s)",
							quote_ident(tableName), quote_ident(columnName),
							datatype, convertedCol);
			statements.add(query);
		}

		if (autoinc != null) {
			// If we're changing the autoincrement status
			if (autoinc) {
				// If we're enabline autoincrement, create a new sequence
				// and attach it to the column, then alter the default
				// value of the column to the next value in the sequence
				sequenceName = generateSequenceName(tableName, columnName);
				statements.add(String.format("CREATE SEQUENCE %s",
						quote_ident(sequenceName)));
				statements.add(String.format(
						"ALTER SEQUENCE %s OWNED BY %s.%s",
						quote_ident(sequenceName), quote_ident(tableName),
						quote_ident(columnName)));
				statements.add(String.format(
						"ALTER TABLE %s ALTER %s SET DEFAULT nextval('%s')",
						quote_ident(tableName), quote_ident(columnName),
						sequenceName));
				// message += String.format(emd.getMessage("Rst066").getText(),
				// columnName);
			} else {
				// If we're disabling autoincrement, remove the default
				// value and drop the sequence.
				statements.add(String.format(
						"ALTER TABLE %s ALTER %s DROP DEFAULT",
						quote_ident(tableName), quote_ident(columnName)));
				statements.add(String.format("DROP SEQUENCE %s", sequenceName));
				// message += String.format(emd.getMessage("Rst067").getText(),
				// columnName);
			}
		}

		// Always do defaultValue after autoinc, in case we're removing
		// an autoinc and adding a default.
		if (defaultValue != null) {
			if (defaultValue.isEmpty()) {
				statements.add(String.format(
						"ALTER TABLE %s ALTER %s DROP DEFAULT",
						quote_ident(tableName), quote_ident(columnName)));
				// message += String.format(emd.getMessage("Rst023").getText(),
				// columnName)+"\n";
			} else {
				statements.add(String.format(
						"ALTER TABLE %s ALTER %s SET DEFAULT %s",
						quote_ident(tableName), quote_ident(columnName),
						quote_literal(defaultValue)));
				// message += String.format(emd.getMessage("Rst024").getText(),
				// columnName,
				// defaultValue)+"\n";
			}
		}
		// Always do the rename last so that everything else works.
		if (newName != null && !newName.isEmpty()) {

			statements.add(String.format("ALTER TABLE %s RENAME %s to %s;",
					quote_ident(tableName), quote_ident(columnName),
					quote_ident(newName)));
			// message += String.format(emd.getMessage("Rst019").getText(),
			// columnName, newName)+"\n";
		}

		// run em in 1 go
		this.runSQLStatements(statements, databaseName, userName, password);
	}


	public void deleteColumn(int dbId, String instance, String tableName,
			String columnName, boolean staging) throws Exception {
		String databaseName = this.dbNameFromIDInstance(dbId, instance, staging);
		String userName = this.getODBCUserName();
		String password = this.getODBCPassword();

		if (!this.checkColumnExists(columnName, tableName, databaseName,
				userName, password)) {
			throw new NotFoundException(String.format(
					"Attempt to delete column %s which doesn't exist!",
					columnName));
		}
		String query = String.format("ALTER TABLE %s DROP %s;",
				quote_ident(tableName), quote_ident(columnName));
		this.runSQLStatement(query, databaseName, userName, password);
	}

	private String generateSequenceName(String tableName, String columnName) {
		return String.format("%s_%s_seq", tableName, columnName);
	}

}
