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

package uk.ac.ox.it.ords.api.database.structure.dto;

public class DatabaseRequest {
	private String databaseName;
	private String databaseServer;
	private int groupId;
	
	// The source database to clone from. Used with PUT to merge a instance into MAIN
	private Integer cloneFrom;
	
	// The instance required, e.g. MAIN, TEST. Used with POSTs
	private String instance;

	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public String getDatabaseServer() {
		return databaseServer;
	}
	public void setDatabaseServer(String databaseServer) {
		this.databaseServer = databaseServer;
	}
	public int getGroupId() {
		return groupId;
	}
	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}
	public Integer getCloneFrom() {
		return cloneFrom;
	}
	public void setCloneFrom(Integer cloneFrom) {
		this.cloneFrom = cloneFrom;
	}
	public String getInstance() {
		return instance;
	}
	public void setInstance(String instance) {
		this.instance = instance;
	}
}
