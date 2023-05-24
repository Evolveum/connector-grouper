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
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.*;

import java.util.List;
import java.util.Map;

public class FilterHandler implements FilterVisitor<ResourceQuery, ResourceQuery> {

    private static final String EQUALS_OP = "=";
    private static final String NOT_EQUALS_OP = "!=";
    // Relational operators
    private static final String LESS_OP = "<";
    private static final String GREATER_OP = ">";
    private static final String LESS_OR_EQ_OP = "<=";
    private static final String GREATER_OR_EQUALS_OP = ">=";

    // Conditional operators

    private static final String AND_OP = "AND";
    private static final String OR_OP = "OR";
    private static final String NOT_OP = "NOT";
    // DELIMITER
    private static final String _L_PAR = "(";
    private static final String _R_PAR = ")";
    private static final String _COL = ",";
    private static final String _S_COL_VALUE_WRAPPER = "'";
    private static final String _PADDING = " ";

    private static final String _IN = "IN";
    private static final String _LIKE = "LIKE";
    private static final String _EXISTS = "EXISTS";
    private static final Log LOG = Log.getLog(FilterHandler.class);

    @Override
    public ResourceQuery visitAndFilter(ResourceQuery r, AndFilter andFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitContainsFilter(ResourceQuery r, ContainsFilter containsFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitContainsAllValuesFilter(ResourceQuery r, ContainsAllValuesFilter containsAllValuesFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitEqualsFilter(ResourceQuery r, EqualsFilter equalsFilter) {
        LOG.ok("Processing through EQUALS filter expression");

        Attribute attr = equalsFilter.getAttribute();

        String snippet = processStringFilter(attr, EQUALS_OP, r);

        r.setQuery(snippet);

        return r;
    }

    @Override
    public ResourceQuery visitExtendedFilter(ResourceQuery r, Filter filter) {
        return null;
    }

    @Override
    public ResourceQuery visitGreaterThanFilter(ResourceQuery r, GreaterThanFilter greaterThanFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitGreaterThanOrEqualFilter(ResourceQuery r,
                                                       GreaterThanOrEqualFilter greaterThanOrEqualFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitLessThanFilter(ResourceQuery r, LessThanFilter lessThanFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitLessThanOrEqualFilter(ResourceQuery r, LessThanOrEqualFilter lessThanOrEqualFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitNotFilter(ResourceQuery r, NotFilter notFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitOrFilter(ResourceQuery r, OrFilter orFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitStartsWithFilter(ResourceQuery r, StartsWithFilter startsWithFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitEndsWithFilter(ResourceQuery r, EndsWithFilter endsWithFilter) {
        return null;
    }

    @Override
    public ResourceQuery visitEqualsIgnoreCaseFilter(ResourceQuery r, EqualsIgnoreCaseFilter equalsIgnoreCaseFilter) {
        return null;
    }

    private String processStringFilter(Attribute attr, String operator, ResourceQuery r) {

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

            String uidName = r.getObjectClassUidName();

            if (uidName == null) {

                throw new ConnectorException("UID name not present in objet type definition in resource query.");
            }


            if (name.equals(Uid.NAME) || name.equals(Name.NAME)) {

                LOG.ok("Property name equals UID or Name value");

                name = uidName;
            }

            query.append(name);
            query.append(_PADDING);
            query.append(operator);
            query.append(_PADDING);
            query.append(wrapValue(r, name, singleValue));
        }

        return query.toString();
    }

    private String wrapValue(ResourceQuery r, String name, String value) {
        LOG.ok("Evaluating value wrapper for the property: {0}", name);


        Map<String, Class> columns = r.getColumnInformation();
        // String uidName = r.getObjectClassUidName();

//        if (name.equals(Uid.NAME) || name.equals(Name.NAME)) {
//
//            LOG.ok("Property name equals UID or Name value");
//
//            name = uidName;
//        }

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

        } else {

            throw new ConnectorException("Query string contains parameter which si not part of the resource schema: " +
                    name);
        }

        throw new ConnectorException("Unexpected exception in value wrapper evaluation duing the proccessing of the" +
                "parameter: " + name);
    }
}
