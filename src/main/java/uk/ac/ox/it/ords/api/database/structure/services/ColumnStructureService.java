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

import uk.ac.ox.it.ords.api.database.structure.metadata.ColumnRequest;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.ColumnStructureServiceImpl;

public interface ColumnStructureService {

	/**
	 * Gets the metadata for a named column in a named table
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param columnName
	 * @return
	 * @throws Exception
	 */
	public ColumnRequest getColumnMetadata(int dbId, String instance,
			String tableName, String columnName, boolean staging)
			throws Exception;

	/**
	 * Creates a new column in a named table
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param column
	 * @throws Exception
	 */
	public void createColumn(int dbId, String instance, String tableName,
			String columnName, ColumnRequest column, boolean staging) throws Exception;

	/**
	 * Updates a named column with the properties in the ColumnRequest object
	 * 
	 * @param dbId
	 * @param intance
	 * @param tableName
	 * @param columnName
	 * @param column
	 * @throws Exception
	 */
	public void updateColumn(int dbId, String intance, String tableName,
			String columnName, ColumnRequest column, boolean staging) throws Exception;

	/**
	 * Deletes a named column
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param column
	 * @throws Exception
	 */
	public void deleteColumn(int dbId, String instance, String tableName,
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
