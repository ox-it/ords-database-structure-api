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

import javax.ws.rs.core.Response;

import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.structure.permissions.DatabaseStructurePermissions;

public class AbstractResource {

	protected boolean canModifyDatabase ( int dbId ) {
		return SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_MODIFY(dbId)
				);
	}
	
	protected boolean canViewDatabase ( int dbId ) {
		return SecurityUtils.getSubject().isPermitted(
				DatabaseStructurePermissions.DATABASE_VIEW(dbId)
				);
	}
	
	protected Response forbidden ( ) {
		return Response.status( Response.Status.FORBIDDEN).build();
	}
	
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
	                case "true":
	                case "yes":
	                case "y":
	                case "staging":{
	                    return BooleanCheck.TRUE;
	                }
	                default: {
	                    return BooleanCheck.FALSE;
	                }
	            }
	        }
	    }
}
