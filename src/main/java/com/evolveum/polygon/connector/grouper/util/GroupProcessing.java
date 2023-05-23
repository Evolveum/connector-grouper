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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class GroupProcessing extends ObjectProcessing {

    private static final Log LOG = Log.getLog(GroupProcessing.class);
    private static final String ATTR_NAME = "group_name";
    private static final String ATTR_DISPLAY_NAME = "display_name";
    private static final String ATTR_DESCRIPTION = "description";
    private static final String ATTR_ID_IDX = "id_index";
    private static final String TABLE_GR_NAME = "gr_mp_groups";
    protected static final Map<String, Class> columns = Map.ofEntries(
            Map.entry(ATTR_NAME, String.class),
            Map.entry(ATTR_DISPLAY_NAME, String.class),
            Map.entry(ATTR_DESCRIPTION, String.class),
            Map.entry(ATTR_ID_IDX, BigInteger.class)
    );
    public GroupProcessing() {

        this.columns.putAll(objectColumns);

    }

    @Override
    public void buildObjectClass(SchemaBuilder schemaBuilder) {


        ObjectClassInfoBuilder subjectObjClassBuilder = new ObjectClassInfoBuilder();
        subjectObjClassBuilder.setType(SUBJECT_NAME);

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

        QueryBuilder queryBuilder = new QueryBuilder(ObjectClass.GROUP_NAME, filter, columns, TABLE_GR_NAME,
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
                LOG.ok("THE RESULT: "+result);
            }

        } catch (SQLException e) {

            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean handleSqlObject(ResultSet resultSet, ResultsHandler handler) throws SQLException {

        //TODO
        return false;
    }
}
