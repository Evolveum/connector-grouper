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

import java.util.*;

public class QueryBuilder {
    private static final Log LOG = Log.getLog(QueryBuilder.class);
    private final String selectTable;
    private static final String _WHERE = "WHERE";
    private static final String _INNER = "INNER";
    private static final String _LEFT = "LEFT";
    private static final String _JOIN = "JOIN";
    private static final String _ON = "ON";
    private static final String _IN = "IN";
    private static final String _LIMIT = "LIMIT";
    private static final String _GROUP_BY = "GROUP BY";
    private static final String _ORDER_BY_ASC = "ORDER BY";
    private static final String _GREATEST = "GREATEST";
    private static final String _MAX = "MAX";
    private static final String _ASC = "ASC";
    private static Integer limit;
    private String joinStatement;
    private ResourceQuery translatedFilter;
    private boolean useFullAlias = false;
    private boolean asSyncQuery = false;

    private Set<String> orderByASC = new HashSet<>();
    private Map<String, Map<String, Class>> columns;

    private Map<Map<String, String>, String> joinPair;

    private Set<String> groupByColumns = new HashSet<>();
    private Map<String, Set<String>> inStatement = new HashMap<>();

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

            if (inStatement != null && !inStatement.isEmpty()) {
                LOG.ok("Empty query parameter, returning full list of objects of the object class: {0}"
                        , objectClass);
            }
            this.translatedFilter = null;
        } else {
            //TODO cleanup

            this.translatedFilter = filter.accept(new FilterHandler(),
                    new ResourceQuery(objectClass, columns));
        }

        if (joinPair != null) {
            if (filter != null && filter instanceof ContainsAllValuesFilter) {

                joinStatement = _INNER + " " + _JOIN;
            } else {

                joinStatement = _LEFT + " " + _JOIN;
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
                    LOG.ok("Augmenting Select, joining with table {0} on the parameter {1}.", joinTable,
                            joinParam);

                    statementString = statementString + " " + joinStatement + " " + joinTable + " " + _ON + " "
                            + selectTable + "." + selectTableJoinParam + " " + "=" + " " + joinTable + "." + joinParam;
                }
            }
        }

        if (translatedFilter != null) {

            statementString = statementString + " " + _WHERE + " " + translatedFilter.getCurrentQuerySnippet();
        }

        if (inStatement != null && !inStatement.isEmpty()) {

            LinkedHashSet<String> inSet = null;
            String queryAttr = null;

            for (String attrNam : inStatement.keySet()) {

                queryAttr = attrNam;
                inSet = (LinkedHashSet<String>) inStatement.get(attrNam);

                //Expecting only one query attribute
                break;
            }

            if (inSet != null && !inSet.isEmpty()) {
            } else {
                throw new ConnectorException("Exception while listing changed accounts for the SYNC operation, " +
                        "list of changed accounts is empty");
            }

            statementString = statementString + " " + _WHERE + " " + queryAttr + " " + _IN + "(";

            Iterator<String> inIterator = inSet.iterator();
            while (inIterator.hasNext()) {

                String inStatement = inIterator.next();
                statementString = statementString + inStatement;

                if (!inIterator.hasNext()) {
                } else {

                    statementString = statementString + ", ";
                }
            }

            statementString = statementString + ")";
        }

        if (limit != null) {

            statementString = statementString + " " + _LIMIT + " " + limit;
        }

        if (asSyncQuery && groupByColumns != null
                && !groupByColumns.isEmpty()) {

            statementString = statementString + " " + _GROUP_BY + "";

            Iterator<String> grpByIterator = groupByColumns.iterator();

            while (grpByIterator.hasNext()) {

                String column = grpByIterator.next();
                statementString = statementString + " " + column;
                if (!grpByIterator.hasNext()) {
                } else {

                    statementString = statementString + ",";
                }
            }
        }

        if (orderByASC != null && !orderByASC.isEmpty()) {

            Iterator<String> orderIterator = orderByASC.iterator();
            statementString = statementString + " " + _ORDER_BY_ASC;
            while (orderIterator.hasNext()) {
                String orderElement = orderIterator.next();

                statementString = statementString + " " + orderElement;
                if (!orderIterator.hasNext()) {
                } else {

                    statementString = statementString + ",";
                }
            }
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
        Set<String> modColumns = new HashSet<>();

        if (tablesAndColumns == null) {

            ret.append("*");
            ret.append(" ");
        } else {

            boolean first = true;

            int noOfTables = tablesAndColumns.keySet().size();

            for (String key : tablesAndColumns.keySet()) {
                Map<String, Class> columnsMap = tablesAndColumns.get(key);

                for (String cName : columnsMap.keySet()) {

                    String name = cName;
                    if (asSyncQuery) {

                        if (ObjectProcessing.ATTR_MODIFIED.equals(name)) {

                            modColumns.add(key + "." + name);
                            continue;
                        } else {

                            groupByColumns.add(key + "." + name);
                        }

                    }

                    LOG.ok("Column name used in select statement: {0}", name);
                    if (!first) {
                        ret.append(", ");
                    }

                    if (noOfTables > 1) {
                        ret.append(key + "." + name);

                        if (useFullAlias) {

                            ret.append(" AS " + key + "$" + name);
                        }

                    } else {

                        ret.append(name);
                    }
                    ret.append(" ");
                    first = false;
                }

            }
        }

        if (asSyncQuery) {
            if (groupByColumns != null && !groupByColumns.isEmpty()) {
                ret.append(",");
            }
            ret.append(buildOneFromMany(modColumns));
        }

        ret.append("FROM ");
        ret.append(selectTable);
        return ret.toString();
    }

    private String buildOneFromMany(Set<String> modColumns) {
        String out = _GREATEST + "(";

        Iterator<String> columnIterator = modColumns.iterator();

        while (columnIterator.hasNext()) {
            String name = columnIterator.next();
            out = out + " " + _MAX + "(" + name + ")";
            if (!columnIterator.hasNext()) {
            } else {
                out = out + " ,";
            }
        }

        out = out + ") AS " + ObjectProcessing.ATTR_MODIFIED_LATEST + " ";

        return out;
    }

    public String buildSyncTokenQuery() {
        asSyncQuery = true;
        String statementString = build();

        statementString = "SELECT " + _MAX + "(" + ObjectProcessing.ATTR_MODIFIED_LATEST + ")" + " FROM ("
                + statementString + ")";
        statementString = statementString + "AS time_max";

        return statementString;
    }

    public boolean isUseFullAlias() {
        return useFullAlias;
    }

    public void setUseFullAlias(boolean useFullAlias) {
        this.useFullAlias = useFullAlias;
    }

    public void setOrderByASC(Set<String> orderByASC) {
        this.orderByASC = orderByASC;
    }

    public void setAsSyncQuery(boolean asSyncQuery) {
        this.asSyncQuery = asSyncQuery;
    }

    public void setInStatement(Map<String, Set<String>> inStatement) {
        this.inStatement = inStatement;
    }


}
