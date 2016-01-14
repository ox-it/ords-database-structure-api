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

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.structure.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.structure.exceptions.NamingConflictException;
import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissions;
import uk.ac.ox.it.ords.api.database.structure.services.ColumnStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.CommentService;
import uk.ac.ox.it.ords.api.database.structure.services.ConstraintService;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.IndexService;
import uk.ac.ox.it.ords.api.database.structure.services.TableStructureService;

public class AbstractResource {

	protected boolean canModifyDatabase(int dbId) {
		return SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_MODIFY(dbId));
	}

	protected boolean canViewDatabase(int dbId) {
		return SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_VIEW(dbId));
	}

	protected Response forbidden() {
		return Response.status(Response.Status.FORBIDDEN).build();
	}

	// Convenience methods to return services
	
	protected IndexService indexServiceInstance() {
		return IndexService.Factory.getInstance();
	}

	
	protected ConstraintService constraintServiceInstance ( ) {
		return ConstraintService.Factory.getInstance();
	}

	
	protected CommentService commentServiceInstance( ) {
		return CommentService.Factory.getInstance();
	}

	
	protected ColumnStructureService columnServiceInstance() {
		return ColumnStructureService.Factory.getInstance();
	}
	

	protected TableStructureService tableServiceInstance() {
		return TableStructureService.Factory.getInstance();
	}
	
	
	protected DatabaseStructureService databaseServiceInstance ( ) {
		return DatabaseStructureService.Factory.getInstance();
	}
	
	// A way of handing exceptions and returning a valid status code
	
	protected Response handleException ( Exception e ) {
		if ( e instanceof BadParameterException ) {
			return Response.status(Response.Status.BAD_REQUEST).entity(e).build();
		}
		else if ( e instanceof NamingConflictException ) {
			return Response.status(Response.Status.CONFLICT).entity(e).build();
		}
		else if ( e instanceof NotFoundException ) {
			return Response.status(Response.Status.NOT_FOUND).entity(e).build();
		}
		else {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
	}
	
	
	// checks for a number of possible permutations for the staging part of the resource path
	public static class BooleanCheck {
		private static final BooleanCheck FALSE = new BooleanCheck(false);
		private static final BooleanCheck TRUE = new BooleanCheck(true);
		private boolean value;

		private BooleanCheck(boolean value) {
			this.value = value;
		}

		public boolean getValue() {
			return this.value;
		}

		public static BooleanCheck valueOf(String value) {
			switch (value.toLowerCase()) {
				case "true" :
				case "yes" :
				case "y" :
				case "staging" : {
					return BooleanCheck.TRUE;
				}
				default : {
					return BooleanCheck.FALSE;
				}
			}
		}
	}
}
