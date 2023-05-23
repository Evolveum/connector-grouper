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
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class QueryBuilder {
    private static final Log LOG = Log.getLog(QueryBuilder.class);
    private final String tableName;
    private String translatedFilter;
    private Map<String, Class> columns;

    public QueryBuilder(String objectClassName, Filter filter, Map columns, String table,
                        OperationOptions operationOptions) {

        if (filter == null) {

            LOG.ok("Empty query parameter, returning full list of objects of the object class: {0}"
                    , objectClassName);
            this.translatedFilter = null;
        } else {

            if (filter != null) {

                this.translatedFilter = filter.accept(new FilterHandler(), new String());
            }
        }

        this.columns = columns;
        this.tableName = table;
    }

    public String build() {
        String statementString = null;
        statementString = select(columns, tableName);

        return statementString;
    }

    private String select(Map<String, Class> columns, String tableName) {

        if (tableName.isEmpty()) {
            throw new ConnectorException("Exception while building select statements for database query, no table name" +
                    "value defined for query.");
        }

        if (columns.isEmpty()) {
            throw new ConnectorException("Exception while building select statements for database query, no column" +
                    "values defined for query.");
        } else {
            StringBuilder ret = new StringBuilder("SELECT ");
            boolean first = true;

            for (String key : columns.keySet()) {
                String name = key;
                if (!first) {
                    ret.append(", ");
                }

                ret.append(name);
                ret.append(" ");
                first = false;
            }

            ret.append("FROM ");
            ret.append(tableName);
            return ret.toString();
        }

    }
}
