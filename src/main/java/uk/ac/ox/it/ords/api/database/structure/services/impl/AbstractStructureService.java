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

package uk.ac.ox.it.ords.api.database.structure.services.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.shiro.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common functions for structure services
 *
 */
public abstract class AbstractStructureService {
	
	Logger log = LoggerFactory.getLogger(AbstractStructureService.class);
	
	/**
	 * Removes quotes from identifiers
	 * @param ident
	 * @return
	 */
	protected String unquote(String ident){
		ident = StringUtils.removeStart(ident, "\"");
		ident = StringUtils.removeEnd(ident, "\"");
		return ident;
	}
	/**
	 * Mimicks the postgres function, surrounding a table or column name in
	 * quotes, escaping existing quotes by doubling them.
	 * 
	 * Note this method is vulnerable to SQL Injection attacks and must only
	 * be used where more secure methods of SQL such as prepared statement 
	 * cannot be used. As an extra precaution we check for an authenticated
	 * user before performing this function.
	 * 
	 * @param ident
	 *            The table, column or other object name.
	 * @return
	 */
	protected String quote_ident(String ident) {
		
		if (!SecurityUtils.getSubject().isAuthenticated()){
			log.warn("Insecure call to quote_ident(). This function should only be called by code that executes for authenticated users due to potential risk of SQL injection.");
		}
		
		if (ident == null) return null;
		
		return "\"" + ident.replace("\"", "\"\"") + "\"";
	}

	/**
	 * Mimicks the postgres function, surrounding a string in quotes, escaping
	 * existing quotes by doubling them.
	 * 
	 * Note this method is vulnerable to SQL Injection attacks and must only
	 * be used where more secure methods of SQL such as prepared statement 
	 * cannot be used. As an extra precaution we check for an authenticated
	 * user before performing this function.
	 * 
	 * As there is no clear case for continuing to support using this insecure
	 * method, it has been marked as Deprecated and will be removed in the 
	 * near future.
	 * 
	 * @param literal
	 * @return
	 */
	@Deprecated
  	protected String quote_literal(String literal){
		
		if (!SecurityUtils.getSubject().isAuthenticated()){
			log.warn("Insecure call to deprecated method quote_literal(). This function should only be called by code that executes for authenticated users due to potential risk of SQL injection.");
		}
		
		if (literal == null) {
			return literal;
		}
		return "'" + literal.replace("'", "''") + "'";
	}

}
