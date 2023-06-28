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
import org.identityconnectors.framework.common.objects.filter.ContainsAllValuesFilter;
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.util.Map;

public class QueryBuilder {
    private static final Log LOG = Log.getLog(QueryBuilder.class);
    private final String selectTable;
    private static final String _WHERE = "WHERE";
    private static final String _INNER = "INNER";
    private static final String _LEFT = "LEFT";
    private static final String _JOIN = "JOIN";
    private static final String _ON = "ON";
    private static final String _LIMIT = "LIMIT";
    private static Integer limit;
    private String joinStatement;
    private ResourceQuery translatedFilter;
    private Map<String, Map<String, Class>> columns;

    //private Map<String, Map<String, String>> joinPair;
    private Map< Map<String, String> ,String> joinPair;

    public QueryBuilder(ObjectClass objectClass, String selectTable, Integer limit) {

        this(objectClass, null, null, selectTable, null, null, limit);
    }

    public QueryBuilder(ObjectClass objectClass, Filter filter, Map<String, Map<String, Class>> columns,
                        String selectTable, OperationOptions oo) {
        this(objectClass, filter, columns, selectTable, null, oo, null);
    }

    public QueryBuilder(ObjectClass objectClass, Filter filter, Map<String, Map<String, Class>> columns,
                        String selectTable, Map<Map<String, String>, String> joinPair,
                        OperationOptions operationOptions) {
        this(objectClass, filter, columns, selectTable, joinPair, operationOptions, null);
    }

    public QueryBuilder(ObjectClass objectClass, Filter filter, Map<String, Map<String, Class>> columns,
                        String selectTable, Map<Map<String, String>, String> joinPair,
                        OperationOptions operationOptions, Integer limit) {

        if (filter == null) {

            LOG.ok("Empty query parameter, returning full list of objects of the object class: {0}"
                    , objectClass);
            this.translatedFilter = null;
        } else {

            if (filter != null) {

                if (joinPair != null) {
                    if (filter instanceof ContainsAllValuesFilter) {

                        joinStatement = _INNER + " " + _JOIN;
                    } else {

                        joinStatement = _LEFT + " " + _JOIN;
                    }
                }

                this.translatedFilter = filter.accept(new FilterHandler(),
                        new ResourceQuery(objectClass, columns));
            }
        }

        this.columns = columns;
        this.selectTable = selectTable;
        this.joinPair = joinPair;
        this.limit = limit;
    }


    public String build() {

        String statementString = select(columns, selectTable);

        if (joinPair != null && !joinPair.isEmpty()) {

            LOG.ok("Starting the parsing of join map.");

            for (Map<String, String> selectTableJoinMap : joinPair.keySet()) {
                //Map<String, String> joinParamTable = selectTableJoinMap;
                String selectTableJoinParam = joinPair.get(selectTableJoinMap);
                //Map<String, String> joinParamTable = joinPair.get(selectTableJoinParam);

                LOG.ok("Parsing join map in regards to join parameter {0} of the table {1}.", selectTableJoinParam,
                        selectTable);

                for (String joinTable : selectTableJoinMap.keySet()) {

                    String joinParam = selectTableJoinMap.get(joinTable);
                    LOG.ok("Augmenting Select, joining with with table {0} on the parameter {1}.", joinTable,
                            joinParam);

                    statementString = statementString + " " + joinStatement + " " + joinTable + " " + _ON + " "
                            + selectTable + "." + selectTableJoinParam + " " + "=" + " " + joinTable + "." + joinParam;
                }
            }
        }

        if (translatedFilter != null) {

            statementString = statementString + " " + _WHERE + " " + translatedFilter.getCurrentQuerySnippet();
        }

        if (limit != null) {

            statementString = statementString + " " + _LIMIT + " " + limit;
        }

        LOG.ok("Using the following statement string in the select statement: {0}", statementString);
        return statementString;
    }

    private String select(Map<String, Map<String, Class>> tablesAndColumns, String selectTable) {


        if (selectTable != null && !selectTable.isEmpty()) {
        } else {

            throw new ConnectorException("Exception while building select statements for database query, no table name" +
                    "value defined for query.");
        }

        StringBuilder ret = new StringBuilder("SELECT ");

        if (tablesAndColumns == null) {
//            throw new ConnectorException("Exception while building select statements for database query, no column" +
//                    "values defined for query.");

            ret.append("*");
            ret.append(" ");
        } else {

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

//            ret.append("FROM ");
//            ret.append(selectTable);
//            return ret.toString();
        }
        ret.append("FROM ");
        ret.append(selectTable);
        return ret.toString();
    }
}
