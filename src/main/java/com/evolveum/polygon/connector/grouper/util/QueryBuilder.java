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
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.util.Map;
import java.util.Set;

public class QueryBuilder {
    private static final Log LOG = Log.getLog(QueryBuilder.class);
    private final String selectTable;
    private static final String _WHERE = "WHERE";
    private static final String _INNER = "INNER";
    private static final String _JOIN = "JOIN";
    private static final String _ON = "ON";
    private ResourceQuery translatedFilter;
    private Map<String, Map<String, Class>> columns;

    private Map<String, Map<String, String>> joinPair;


//    public QueryBuilder(ObjectClass objectClass, Filter filter, Map<String, Map<String, Class>> columns,
//                        String selectTable, String uniqueName) {
//        this(objectClass, filter, columns, selectTable, uniqueName,null,  null);
//    }

    public QueryBuilder(ObjectClass objectClass, Filter filter, Map<String, Map<String, Class>> columns,
                        String selectTable, OperationOptions oo) {
        this(objectClass, filter, columns, selectTable, null, oo);
    }

    public QueryBuilder(ObjectClass objectClass, Filter filter, Map<String, Map<String, Class>> columns,
                        String selectTable, Map<String, Map<String, String>> joinPair,
                        OperationOptions operationOptions) {

        if (filter == null) {

            LOG.ok("Empty query parameter, returning full list of objects of the object class: {0}"
                    , objectClass);
            this.translatedFilter = null;
        } else {

            if (filter != null) {

                this.translatedFilter = filter.accept(new FilterHandler(),
                        new ResourceQuery(objectClass, columns));
            }
        }

        this.columns = columns;
        this.selectTable = selectTable;
        this.joinPair = joinPair;
    }

    public String build() {

        String statementString = select(columns, selectTable);

        if (joinPair != null && !joinPair.isEmpty()) {

            LOG.ok("Starting the parsing of join map.");

            for (String selectTableJoinParam : joinPair.keySet()) {

                Map<String, String> joinParamTable = joinPair.get(selectTableJoinParam);
                LOG.ok("Parsing join map in regards to join parameter {0} of the table {1}.", selectTableJoinParam,
                        selectTable);

                for (String joinTable : joinParamTable.keySet()) {

                    String joinParam = joinParamTable.get(joinTable);
                    LOG.ok("Augmenting Select, joining with with table {0} on the parameter {1}.", joinTable,
                            joinParam);

                    statementString = statementString + " " + _INNER + " " + _JOIN + " " + joinTable + " " + _ON + " "
                            + selectTable + "." + selectTableJoinParam + " " + "=" + " " + joinTable + "." + joinParam;
                }
            }
        }

        if (translatedFilter != null) {

            statementString = statementString + " " + _WHERE + " " + translatedFilter.getQuery();
        }

        LOG.ok("Using the following statement string in the select statement: {0}.", statementString);
        return statementString;
    }

    private String select(Map<String, Map<String, Class>> tablesAndColumns, String selectTable) {


        if (selectTable != null && !selectTable.isEmpty()) {
        } else {

            throw new ConnectorException("Exception while building select statements for database query, no table name" +
                    "value defined for query.");
        }

        if (tablesAndColumns.isEmpty()) {
            throw new ConnectorException("Exception while building select statements for database query, no column" +
                    "values defined for query.");
        } else {
            StringBuilder ret = new StringBuilder("SELECT ");
            boolean first = true;

            int noOfTables = tablesAndColumns.keySet().size();

            for (String key : tablesAndColumns.keySet()) {
                Map<String, Class> columnsMap = tablesAndColumns.get(key);

                for (String cName : columnsMap.keySet()) {

                    String name = cName;
                    LOG.ok("Column name used in select statement: {0}", name);
                    if (!first) {
                        ret.append(", ");
                    }

                    if (noOfTables > 1) {
                        ret.append(key + "." + name);
                    } else {

                        ret.append(name);
                    }
                    ret.append(" ");
                    first = false;
                }

            }

            ret.append("FROM ");
            ret.append(selectTable);
            return ret.toString();
        }

    }
}
