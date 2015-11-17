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

package uk.ac.ox.it.ords.api.database.structure.metadata;

import java.util.ArrayList;



public class PositionRequest {
    /**
     * List of the updated table positions as they are laid out in the schema
     * designer
     */
    private ArrayList<TablePosition> positions;

    public PositionRequest() {
    }

    public ArrayList<TablePosition> getPositions() {
        return positions;
    }

    public void setPositions(ArrayList<TablePosition> positions) {
        this.positions = positions;
    }

}
