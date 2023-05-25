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
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.sql.*;
import java.util.*;

public class SubjectProcessing extends ObjectProcessing {

    private static final Log LOG = Log.getLog(SubjectProcessing.class);
    private static final String ATTR_ID = "subject_id";
    private static final String ATTR_ID_IDX = "subject_id_index";
    private static final String ATTR_MEMBER_OF = "member_of";
    private static final String TABLE_SU_NAME = "gr_mp_subjects";
    private static final ObjectClass O_CLASS = new ObjectClass(SUBJECT_NAME);
    private static final String ATTR_UID = ATTR_ID_IDX;
    protected Map<String, Class> columns = new HashMap<>();


    public SubjectProcessing() {

        columns.put(ATTR_ID_IDX, Long.class);
        columns.put(ATTR_ID, String.class);
        this.columns.putAll(objectColumns);

    }

    @Override
    public void buildObjectClass(SchemaBuilder schemaBuilder) {


        ObjectClassInfoBuilder subjectObjClassBuilder = new ObjectClassInfoBuilder();
        subjectObjClassBuilder.setType(SUBJECT_NAME);

        //subjectObjClassBuilder.addAttributeInfo(OperationalAttributeInfos.ENABLE);
        //subjectObjClassBuilder.addAttributeInfo(OperationalAttributeInfos.PASSWORD);

        //Read-only,
        AttributeInfoBuilder id = new AttributeInfoBuilder(ATTR_ID);
        id.setRequired(true).setType(String.class).setCreateable(false).setUpdateable(false).setReadable(true);
        subjectObjClassBuilder.addAttributeInfo(id.build());

        AttributeInfoBuilder last_modified = new AttributeInfoBuilder(ATTR_MODIFIED);
        last_modified.setRequired(false).setType(Integer.class).setCreateable(false).setUpdateable(false).setReadable(true);
        subjectObjClassBuilder.addAttributeInfo(last_modified.build());

        AttributeInfoBuilder deleted = new AttributeInfoBuilder(ATTR_DELETED);
        deleted.setRequired(false).setType(Integer.class).setCreateable(false).setUpdateable(false).setReadable(true);
        subjectObjClassBuilder.addAttributeInfo(deleted.build());

        AttributeInfoBuilder memberOf = new AttributeInfoBuilder(ATTR_MEMBER_OF);
        memberOf.setRequired(false).setType(String.class).setMultiValued(true)
                .setCreateable(false).setUpdateable(false).setReadable(true)
                .setReturnedByDefault(false)
                .build();


        schemaBuilder.defineObjectClass(subjectObjClassBuilder.build());
    }

    public void executeQuery(Filter filter, ResultsHandler handler, OperationOptions operationOptions
            , Connection connection) {
        LOG.ok("Processing trough executeQuery methods for the object class {0}",
                SUBJECT_NAME);

        QueryBuilder queryBuilder = new QueryBuilder(new ObjectClass(SUBJECT_NAME), filter, columns, TABLE_SU_NAME,
                ATTR_ID_IDX, operationOptions);
        String query = queryBuilder.build();
        ResultSet result = null;

        LOG.info("Query about to be executed: {0}", query);

        try {
            //TODO
            LOG.ok("TODO about to execute prepared statement");
            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();

            while (result.next()) {
                // create the connector object
                //TODO
                LOG.ok("TODO scanning result set");
                LOG.ok(result.toString());
                ConnectorObjectBuilder co = buildConnectorObject(O_CLASS, ATTR_UID, result, operationOptions,
                        objectColumns);
                if (getAttributesToGet(operationOptions).contains(ATTR_MEMBER_OF)) {

                    if (filter instanceof EqualsFilter) {
                        LOG.ok("Processing Equals Query");
                        final EqualsFilter equalsFilter = (EqualsFilter) filter;
                        Attribute fAttr = equalsFilter.getAttribute();

                        if (Uid.NAME.equals(fAttr.getName())) {

                            LOG.ok("Processing Equals Query based on UID.");
                            String uid = ((Uid) fAttr).getUidValue();
                            populateMembershipAttribute(uid, co, connection);
                        } else {

                            //TODO error
                        }
                    } else {

                        // TODO error
                    }
                }

                handler.handle(co.build());
            }

        } catch (SQLException e) {

            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ConnectorObjectBuilder populateMembershipAttribute(String uid, ConnectorObjectBuilder ob,
                                                                 Connection connection) throws SQLException {

        String uidValue = ob.build().getUid().getUidValue();
        EqualsFilter equalsFilter = new EqualsFilter(AttributeBuilder.build(ATTR_SCT_ID_IDX, uidValue));
        Map<String, Class> idAttrDescriptorMap = Collections.singletonMap(ATTR_SCT_ID_IDX, Long.class);
        Map<String, Class> membAttrDescriptorMap = Collections.singletonMap(ATTR_GR_ID_IDX, Long.class);

        QueryBuilder queryBuilder = new QueryBuilder(O_CLASS, equalsFilter, idAttrDescriptorMap, membAttrDescriptorMap,
                TABLE_MEMBERSHIP_NAME, ATTR_ID_IDX);

        String query = queryBuilder.build();
        LOG.info("Query about to be executed for membership fetch: {0}", query);

        PreparedStatement preparedStatement = connection.prepareStatement(query);
        ResultSet result = preparedStatement.executeQuery();

        HashMap<String, Set<Object>> multiValues = new HashMap<>();

        while (result.next()) {

            buildMultiValued(result, membAttrDescriptorMap, multiValues);
        }

        for (String attrName : multiValues.keySet()) {
            LOG.ok("Adding attribute values for the attribute {0} to the attribute builder.", attrName);

            if (ATTR_GR_ID_IDX.equals(attrName)){

                ob.addAttribute(ATTR_MEMBER_OF, multiValues.get(attrName));
            }
        }

        return ob;
    }

    // TODO pull to parent
    private void buildMultiValued(ResultSet result, Map<String, Class> columns, HashMap<String, Set<Object>> multiValues)
            throws SQLException {

        LOG.info("Evaluation of SQL objects present in result set for multivalued attributes.");

        ResultSetMetaData meta = result.getMetaData();

        int count = meta.getColumnCount();
        LOG.ok("Number of columns returned from result set object: {0}", count);
        // TODO Based on options the handling might be paginated
        // options

        for (int i = 1; i <= count; i++) {
            String name = meta.getColumnName(i);
            LOG.ok("Evaluation of column with name {0}", name);
            Set<Object> attrValues = new HashSet<>();

            if (multiValues.containsKey(name)) {

                attrValues = multiValues.get(name);
            }

            if (columns.containsKey(name)) {
                Class type = columns.get(name);

                if (type.equals(Long.class)) {

                    LOG.ok("Addition of Long type attribute for attribute from column with name {0} to the multivalued" +
                            " collection", name);

                    attrValues.add(result.getLong(i));
                    multiValues.put(name, attrValues);
                }

                if (type.equals(String.class)) {

                    LOG.ok("Addition of String type attribute for attribute from column with name {0} to the multivalued" +
                            " collection", name);

                    attrValues.add(result.getString(i));
                    multiValues.put(name, attrValues);
                }

            } else {

                LOG.info("SQL object handling discovered during multivalued attribute evaluation a column which is not" +
                        " present in the original schema set. The column name: {0}", name);
            }
        }

    }
}
