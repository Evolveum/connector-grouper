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
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.math.BigInteger;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class GroupProcessing extends ObjectProcessing {

    private static final Log LOG = Log.getLog(GroupProcessing.class);
    private static final String ATTR_NAME = "group_name";
    private static final String ATTR_DISPLAY_NAME = "display_name";
    private static final String ATTR_DESCRIPTION = "description";
    private static final String ATTR_ID_IDX = "id_index";
    private static final String TABLE_GR_NAME = "gr_mp_groups";
    protected Map<String, Class> columns = new HashMap<>();

    public GroupProcessing() {
        columns.put(ATTR_NAME, String.class);
        columns.put(ATTR_DISPLAY_NAME, String.class);
        columns.put(ATTR_DESCRIPTION, String.class);
        columns.put(ATTR_ID_IDX, Long.class);
        this.columns.putAll(objectColumns);

    }

    @Override
    public void buildObjectClass(SchemaBuilder schemaBuilder) {


        ObjectClassInfoBuilder subjectObjClassBuilder = new ObjectClassInfoBuilder();
        subjectObjClassBuilder.setType(ObjectClass.GROUP_NAME);

        //subjectObjClassBuilder.addAttributeInfo(OperationalAttributeInfos.ENABLE);
        //subjectObjClassBuilder.addAttributeInfo(OperationalAttributeInfos.PASSWORD);

        //Read-only,

        AttributeInfoBuilder id_index = new AttributeInfoBuilder(ATTR_ID_IDX);
        id_index.setRequired(true).setType(Integer.class).setCreateable(false).setUpdateable(false).setReadable(true);
        subjectObjClassBuilder.addAttributeInfo(id_index.build());

        AttributeInfoBuilder name = new AttributeInfoBuilder(ATTR_NAME);
        name.setRequired(false).setType(String.class).setCreateable(false).setUpdateable(false).setReadable(true);
        subjectObjClassBuilder.addAttributeInfo(name.build());

        AttributeInfoBuilder display_name = new AttributeInfoBuilder(ATTR_DISPLAY_NAME);
        display_name.setRequired(false).setType(String.class).setCreateable(false).setUpdateable(false).setReadable(true);
        subjectObjClassBuilder.addAttributeInfo(display_name.build());

        AttributeInfoBuilder description = new AttributeInfoBuilder(ATTR_DESCRIPTION);
        description.setRequired(false).setType(String.class).setCreateable(false).setUpdateable(false).setReadable(true);
        subjectObjClassBuilder.addAttributeInfo(description.build());

        AttributeInfoBuilder last_modified = new AttributeInfoBuilder(ATTR_MODIFIED);
        last_modified.setRequired(false).setType(Integer.class).setCreateable(false).setUpdateable(false).setReadable(true);
        subjectObjClassBuilder.addAttributeInfo(last_modified.build());

        AttributeInfoBuilder deleted = new AttributeInfoBuilder(ATTR_DELETED);
        deleted.setRequired(false).setType(Integer.class).setCreateable(false).setUpdateable(false).setReadable(true);
        subjectObjClassBuilder.addAttributeInfo(deleted.build());

        schemaBuilder.defineObjectClass(subjectObjClassBuilder.build());
    }

    public void executeQuery(Filter filter, ResultsHandler handler, OperationOptions operationOptions
            , Connection connection) {
        LOG.ok("Processing trough executeQuery methods for the object class {0}",
                ObjectClass.GROUP_NAME);

        QueryBuilder queryBuilder = new QueryBuilder(ObjectClass.GROUP, filter, columns, TABLE_GR_NAME, ATTR_ID_IDX,
                operationOptions);
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
                LOG.ok("THE RESULT: " + result);
            }

        } catch (SQLException e) {

            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean handleSqlObject(ResultSet resultSet, ResultsHandler handler, OperationOptions oo)
            throws SQLException {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();

        ResultSetMetaData meta = resultSet.getMetaData();

        int count = meta.getColumnCount();

        // TODO Based on options the handling might be paginated
        // options

        for (int i = 1; i <= count; i++) {
            String name = meta.getColumnName(i);

            if (name != ATTR_ID_IDX) {


                if (columns.containsKey(name)) {
                    Class type = columns.get(name);

                    if (type.equals(Long.class)) {

                        builder.addAttribute(name, resultSet.getLong(i));
                    }

                    if (type.equals(String.class)) {

                        builder.addAttribute(name, resultSet.getString(i));
                    }

                } else {

                    LOG.info("SQL object handling discovered a column which is not present in the original schema set. " +
                            "The column name: {0}", name);
                }
            } else {
                builder.setName(Long.toString(resultSet.getLong(i)));
                builder.setUid(Long.toString(resultSet.getLong(i)));

            }

        }

        return handler.handle(builder.build());
    }
}
