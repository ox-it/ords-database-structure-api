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

import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.structure.dto.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.ColumnStructureServiceImpl;

public interface ColumnStructureService {

	/**
	 * Gets the metadata for a named column in a named table
	 * 
	 * @param database the database
	 * @param tableName the table
	 * @param columnName the column
	 * @param staging whether this is a staging version
	 * @return the column metadata
	 * @throws Exception if there is a problem obtaining the column
	 */
	public ColumnRequest getColumnMetadata(OrdsPhysicalDatabase database,
			String tableName, String columnName, boolean staging)
			throws Exception;

	/**
	 * Creates a new column in a named table
	 * 
	 * @param database the database
	 * @param tableName the table
	 * @param columnName the column name
	 * @param column the column request
	 * @param staging  whether this is a staging version
	 * @throws Exception if there is a problem creating the column
	 */
	public void createColumn(OrdsPhysicalDatabase database, String tableName,
			String columnName, ColumnRequest column, boolean staging) throws Exception;

	/**
	 * Updates a named column with the properties in the ColumnRequest object
	 * 
	 * @param database the database
	 * @param tableName the table
	 * @param columnName the column
	 * @param column the request
	 * @param staging  whether this is a staging version
	 * @throws Exception if there is a problem updating the column
	 */
	public void updateColumn(OrdsPhysicalDatabase database, String tableName,
			String columnName, ColumnRequest column, boolean staging) throws Exception;

	/**
	 * Deletes a named column
	 * 
	 * @param database the database
	 * @param tableName the table
	 * @param columnName the column
	 * @param staging  whether this is a staging version
	 * @throws Exception if there is a problem deleting the column
	 */
	public void deleteColumn(OrdsPhysicalDatabase database, String tableName,
			String columnName, boolean staging) throws Exception;

	/**
	 * Factory for obtaining implementations
	 */
	public static class Factory {
		private static ColumnStructureService provider;
		public static ColumnStructureService getInstance() {
			//
			// Use the service loader to load an implementation if one is
			// available
			// Place a file called
			// uk.ac.ox.it.ords.api.structure.service.ColumnStructureService in
			// src/main/resources/META-INF/services
			// containing the classname to load as the ColumnStructureService
			// implementation.
			// By default we load the Hibernate implementation.
			//
			if (provider == null) {
				ServiceLoader<ColumnStructureService> ldr = ServiceLoader
						.load(ColumnStructureService.class);
				for (ColumnStructureService service : ldr) {
					// We are only expecting one
					provider = service;
				}
			}
			//
			// If no service provider is found, use the default
			//
			if (provider == null) {
				provider = new ColumnStructureServiceImpl();
			}

			return provider;
		}
	}

}
