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


public class ColumnRequest {
    
    /**
     *  If set, this will be used to change the name of the column
     */
    private String newname;
    
    /**
     *  If false, the column will be set to NOT NULL
     */
    private Boolean nullable;
    
    /**
     *  The data type, in PostgreSQL terms
     */
    private String datatype;
    
    /**
     * The default value for the column. Not set for none, set to null for null.
     * Cannot be set to null unless nullable is true.
     * Cannot be set to any value if autoincrement is true.
     */
    private String defaultvalue;
    
    /**
     * If true, a sequence will be created owned by this column, and the column
     * will default to the next value in that sequence.
     */
    private Boolean autoincrement;

    public String getNewname() {
        return newname;
    }

    public void setNewname(String newname) {
        this.newname = newname;
    }

    public Boolean isNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public String getDefaultvalue() {
        return defaultvalue;
    }

    public void setDefaultvalue(String defaultvalue) {
        this.defaultvalue = defaultvalue;
    }

    public Boolean isAutoincrement() {
        return autoincrement;
    }

    public void setAutoincrement(Boolean autoincrement) {
        this.autoincrement = autoincrement;
    }

    public ColumnRequest() {
    }
    
}
