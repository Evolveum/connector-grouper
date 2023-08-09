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
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.*;

import java.util.*;

public class FilterHandler implements FilterVisitor<ResourceQuery, ResourceQuery> {

    private static final String EQUALS_OP = "=";
    private static final String LESS_OP = "<";
    private static final String GREATER_OP = ">";
    private static final String LESS_OR_EQ_OP = "<=";
    private static final String GREATER_OR_EQUALS_OP = ">=";

    // Conditional operators

    private static final String AND_OP = "AND";
    private static final String OR_OP = "OR";
    private static final String NOT_OP = "NOT";
    // DELIMITER

    private static final String _S_COL_VALUE_WRAPPER = "'";
    private static final String _PADDING = " ";

    private static final String _LIKE = "LIKE";
    private static final Log LOG = Log.getLog(FilterHandler.class);

    @Override
    public ResourceQuery visitAndFilter(ResourceQuery r, AndFilter andFilter) {

        LOG.ok("Processing through AND filter expression");

        Collection<Filter> filters = andFilter.getFilters();

        processCompositeFilter(filters, AND_OP, r);


        return r;
    }

    @Override
    public ResourceQuery visitContainsFilter(ResourceQuery r, ContainsFilter containsFilter) {
        LOG.ok("Processing through CONTAINS filter expression");

        Attribute attr = containsFilter.getAttribute();

        String snippet = processStringFilter(attr, _LIKE, r, containsFilter);

        r.setCurrentQuerySnippet(snippet);

        return r;
    }

    @Override
    public ResourceQuery visitContainsAllValuesFilter(ResourceQuery r, ContainsAllValuesFilter containsAllValuesFilter) {
        LOG.ok("Processing through CONTAINS ALL VALUES filter expression");

        Attribute attr = containsAllValuesFilter.getAttribute();
        String snippet = processStringFilter(attr, EQUALS_OP, r);
        r.setCurrentQuerySnippet(snippet);

        return r;
    }

    @Override
    public ResourceQuery visitEqualsFilter(ResourceQuery r, EqualsFilter equalsFilter) {
        LOG.ok("Processing through EQUALS filter expression");

        Attribute attr = equalsFilter.getAttribute();

        String snippet = processStringFilter(attr, EQUALS_OP, r);

        r.setCurrentQuerySnippet(snippet);

        return r;
    }

    @Override
    public ResourceQuery visitExtendedFilter(ResourceQuery r, Filter filter) {
        throw new ConnectorException("Filter 'EXTENDED FILTER' not implemented by the connector. ");
    }

    @Override
    public ResourceQuery visitGreaterThanFilter(ResourceQuery r, GreaterThanFilter greaterThanFilter) {

        LOG.ok("Processing through GREATER_THAN filter expression");

        Attribute attr = greaterThanFilter.getAttribute();

        String snippet = processStringFilter(attr, GREATER_OP, r);

        r.setCurrentQuerySnippet(snippet);

        return r;

    }

    @Override
    public ResourceQuery visitGreaterThanOrEqualFilter(ResourceQuery r,
                                                       GreaterThanOrEqualFilter greaterThanOrEqualFilter) {
        LOG.ok("Processing through GREATER_THAN_OR_EQUAL filter expression");

        Attribute attr = greaterThanOrEqualFilter.getAttribute();

        String snippet = processStringFilter(attr, GREATER_OR_EQUALS_OP, r);

        r.setCurrentQuerySnippet(snippet);

        return r;
    }

    @Override
    public ResourceQuery visitLessThanFilter(ResourceQuery r, LessThanFilter lessThanFilter) {
        LOG.ok("Processing through LESS THAN filter expression");

        Attribute attr = lessThanFilter.getAttribute();

        String snippet = processStringFilter(attr, LESS_OP, r);

        r.setCurrentQuerySnippet(snippet);

        return r;
    }

    @Override
    public ResourceQuery visitLessThanOrEqualFilter(ResourceQuery r, LessThanOrEqualFilter lessThanOrEqualFilter) {

        LOG.ok("Processing through LESS_THAN_OR_EQUAL filter expression");

        Attribute attr = lessThanOrEqualFilter.getAttribute();

        String snippet = processStringFilter(attr, LESS_OR_EQ_OP, r);

        r.setCurrentQuerySnippet(snippet);

        return r;
    }

    @Override
    public ResourceQuery visitNotFilter(ResourceQuery r, NotFilter notFilter) {

        LOG.ok("Processing through NOT filter expression");

        Collection<Filter> filters = Collections.singleton(notFilter.getFilter());

        processCompositeFilter(filters, NOT_OP, r);

        r.addOperator(NOT_OP);

        return r;
    }

    @Override
    public ResourceQuery visitOrFilter(ResourceQuery r, OrFilter orFilter) {

        LOG.ok("Processing through OR filter expression");

        Collection<Filter> filters = orFilter.getFilters();

        processCompositeFilter(filters, OR_OP, r);


        return r;

    }

    @Override
    public ResourceQuery visitStartsWithFilter(ResourceQuery r, StartsWithFilter startsWithFilter) {

        LOG.ok("Processing through STARTS WITH filter expression");

        Attribute attr = startsWithFilter.getAttribute();

        String snippet = processStringFilter(attr, _LIKE, r, startsWithFilter);

        r.setCurrentQuerySnippet(snippet);

        return r;
    }

    @Override
    public ResourceQuery visitEndsWithFilter(ResourceQuery r, EndsWithFilter endsWithFilter) {

        LOG.ok("Processing through ENDS WITH filter expression");

        Attribute attr = endsWithFilter.getAttribute();

        String snippet = processStringFilter(attr, _LIKE, r, endsWithFilter);

        r.setCurrentQuerySnippet(snippet);

        return r;

    }

    @Override
    public ResourceQuery visitEqualsIgnoreCaseFilter(ResourceQuery r, EqualsIgnoreCaseFilter equalsIgnoreCaseFilter) {
        throw new ConnectorException("Filter 'EQUALS IGNORE CASE FILTER' not implemented by the connector. ");
    }

    private String processStringFilter(Attribute attr, String operator, ResourceQuery r) {

        return processStringFilter(attr, operator, r, null);
    }

    private String processStringFilter(Attribute attr, String operator, ResourceQuery r, Filter filter) {

        StringBuilder query = new StringBuilder();

        if (attr != null) {
            String singleValue = null;
            String name = attr.getName();
            List value = attr.getValue();

            if (value != null && !value.isEmpty()) {

                singleValue = AttributeUtil.getSingleValue(attr).toString();

            } else {

                LOG.error("Unexpected error, attribute {0} without a value.", name);
            }

            name = evaluateNonNativeAttributeNames(r, name);

            Map<String, Map<String, Class>> tableAndcolumns = r.getColumnInformation();
            String wrappedValue = null;
            Iterator<String> iterator = tableAndcolumns.keySet().iterator();

            while (iterator.hasNext()) {
                String tableName = iterator.next();
                Map<String, Class> columns = tableAndcolumns.get(tableName);

                String attrName;

                if (name.contains(".")) {
                    String[] nameParts = name.split("\\.");
                    String tableNamePart = nameParts[0];

                    if (!tableName.equals(tableNamePart)) {
                        continue;
                    }

                    attrName = nameParts[1];

                } else {

                    attrName = name;
                }

                if (columns.containsKey(name) || attrName != null && columns.containsKey(attrName)) {

                    LOG.ok("Original attribute name value: {0}", name);
                    LOG.ok("Wrapping the value {0}, and filter construction for the attribute {1} of the table {2}",
                            singleValue, attrName, tableName);

                    wrappedValue = wrapValue(columns, attrName, singleValue, filter);

                    LOG.ok("Wrapped attribute name value: {0}", wrappedValue);
                    name = name.contains(".") ? name : tableName + "." + name;
                    break;
                } else {
                    if (!iterator.hasNext()) {

                        throw new ConnectorException("Unexpected exception in string filter processing," +
                                " during the processing of the parameter: " + name + " for the table: " + tableName);
                    }
                }
            }


            if (filter != null) {

                if (filter instanceof ContainsFilter || filter instanceof StartsWithFilter ||
                        filter instanceof EndsWithFilter) {

                    name = name + "::TEXT";
                }
            }

            query.append(name);
            query.append(_PADDING);
            query.append(operator);
            query.append(_PADDING);
            query.append(wrappedValue);
        }

        LOG.ok("Query snippet value: {0}", query);
        return query.toString();
    }


    private String evaluateNonNativeAttributeNames(ResourceQuery r, String name) {

        if (Uid.NAME.equals(name)) {

            LOG.ok("Property name equals UID value");
            ObjectClass oc = r.getObjectClass();

            if (oc.is(ObjectClass.GROUP_NAME)) {

                return GroupProcessing.ATTR_UID;
            } else {

                return SubjectProcessing.ATTR_UID;
            }

        }

        if (Name.NAME.equals(name)) {

            LOG.ok("Property name equals Name value");

            ObjectClass oc = r.getObjectClass();

            if (oc.is(ObjectClass.GROUP_NAME)) {

                return GroupProcessing.ATTR_NAME;
            } else {

                return SubjectProcessing.ATTR_NAME;
            }
        }


        if (GroupProcessing.ATTR_MEMBERS.equals(name) || SubjectProcessing.ATTR_MEMBER_OF.equals(name)) {

            ObjectClass oc = r.getObjectClass();
            if (oc.is(ObjectClass.GROUP_NAME)) {

                return GroupProcessing.ATTR_MEMBERS_NATIVE;
            } else {

                return SubjectProcessing.ATTR_MEMBER_OF_NATIVE;
            }
        }

        return name;
    }

    private void processCompositeFilter(Collection<Filter> filters, String op, ResourceQuery r) {

        ResourceQuery query = new ResourceQuery(r.getObjectClass(), r.getColumnInformation());

        Iterator<Filter> filterIterator = filters.iterator();

        while (filterIterator.hasNext()) {

            Filter filter = filterIterator.next();
            if (filter instanceof CompositeFilter || filter instanceof NotFilter) {
                ResourceQuery compositeQuery = new ResourceQuery(r.getObjectClass(), r.getColumnInformation());
                compositeQuery.setComposite(true);

                r.add(filter.accept(this, compositeQuery), op);
            } else {

                r.add(filter.accept(this, query), op);
            }
        }

    }


    private String wrapValue(Map<String, Class> columns, String name, String value, Filter filter) {
        LOG.ok("Evaluating value wrapper for the property: {0}", name);

        if (filter != null) {

            if (filter instanceof ContainsFilter) {

                return _S_COL_VALUE_WRAPPER + "%" + value + "%" + _S_COL_VALUE_WRAPPER;

            } else if (filter instanceof StartsWithFilter) {

                return _S_COL_VALUE_WRAPPER + value + "%" + _S_COL_VALUE_WRAPPER;

            } else if (filter instanceof EndsWithFilter) {

                return _S_COL_VALUE_WRAPPER + "%" + value + _S_COL_VALUE_WRAPPER;
            }

        }
        if (columns.containsKey(name)) {

            Class type = columns.get(name);

            if (type.equals(Long.class)) {

                LOG.ok("Addition of Long type attribute for attribute from column with name {0}", name);
                return value;
            }

            if (type.equals(String.class)) {

                LOG.ok("Addition of String type attribute for attribute from column with name {0}", name);
                return _S_COL_VALUE_WRAPPER + value + _S_COL_VALUE_WRAPPER;
            }

        }

        throw new ConnectorException("Unexpected exception in value wrapper evaluation during the processing of the" +
                "parameter: " + name);
    }
}
