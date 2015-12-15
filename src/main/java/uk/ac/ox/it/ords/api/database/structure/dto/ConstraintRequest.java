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

import java.util.ArrayList;


public class ConstraintRequest {
    /**
     * The new name of the constraint, if it is being editied
     */
    private String newname;
    
    /**
     * True of this is a unique constraint, false otherwise
     */
    private Boolean unique;

    /**
     * True if this is a Primary Key constraint, false otherwise
     */
    private Boolean primary;

    /**
     * True of this is a foreign key constraint, false otherwise
     */
    private Boolean foreign;

    /**
     * The columns in the table specified in the URL that are constrained
     * 
     * Can be more than one column for a Unique or Primary Key constraint,
     * but only one for a Foreign Key,
     */
    private ArrayList<String> columns;

    /**
     * The table that is checked for referential integrity, if this is a 
     * foreign key constraint
     */
    private String reftable;

    /**
     * The column that is checked for referential integrity, if this is a
     * foreign key constraint.
     */
    private String refcolumn;

    /**
     * If not a Unique, Primary or Foreign Key, then this is the check expression
     * for the constraint. Not currently in use, but defined here so it can
     * be added at a later date for database-level data validation.
     */
    private String checkexpression;

    public ConstraintRequest() {
    }

    public String getNewname() {
        return newname;
    }

    public void setNewname(String newname) {
        this.newname = newname;
    }

    public Boolean isUnique() {
        return unique;
    }

    public void setUnique(Boolean unique) {
        this.unique = unique;
    }

    public Boolean isPrimary() {
        return primary;
    }

    public void setPrimary(Boolean isPrimary) {
        this.primary = isPrimary;
    }

    public Boolean isForeign() {
        return foreign;
    }

    public void setForeign(Boolean isForeign) {
        this.foreign = isForeign;
    }

    public ArrayList<String> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<String> columns) {
        this.columns = columns;
    }

    public void setColumn(String column) {
        columns = new ArrayList<String>();
        columns.add(column);
    }

    public String getColumn() {
        return columns.get(0);
    }
    

    public String getReftable() {
        return reftable;
    }

    public void setReftable(String reftable) {
        this.reftable = reftable;
    }

    public String getRefcolumn() {
        return refcolumn;
    }

    public void setRefcolumn(String column) {
        this.refcolumn = column;
    }

    public String getCheckExpression() {
        return checkexpression;
    }

    public void setCheckExpression(String checkExpression) {
        this.checkexpression = checkExpression;
    }

}
