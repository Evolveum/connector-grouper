/*
 * Copyright (c) 2010-2023 Evolveum
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

package com.evolveum.polygon.connector.grouper.util;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.ObjectClass;

import java.util.Map;

public class ResourceQuery {
    private static final Log LOG = Log.getLog(ResourceQuery.class);
    private ObjectClass objectClass;
    private Map<String, Map<String, Class>> columnInformation;

    private String query = null;

    public ResourceQuery(ObjectClass objectClass, Map<String, Map<String, Class>> columnInformation) {

        this.objectClass = objectClass;
        this.columnInformation = columnInformation;

    }

    public ObjectClass getObjectClass() {
        return objectClass;
    }

    public Map<String, Map<String, Class>> getColumnInformation() {
        return columnInformation;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

}
