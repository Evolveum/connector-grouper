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
import java.util.Map;

public class SubjectProcessing extends ObjectProcessing {

    private static final Log LOG = Log.getLog(SubjectProcessing.class);
    private static final String ATTR_ID = "subject_id";
    private static final String ATTR_ID_IDX = "subject_id_index";
    private static final String TABLE_SU_NAME = "gr_mp_subjects";
    protected static final Map<String, Class> columns = Map.ofEntries(
            Map.entry(ATTR_ID_IDX, BigInteger.class),
            Map.entry(ATTR_ID, String.class)
    );


    public SubjectProcessing() {
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


        schemaBuilder.defineObjectClass(subjectObjClassBuilder.build());
    }

    public void executeQuery(Filter filter, ResultsHandler handler, OperationOptions operationOptions
            , Connection connection) {
        LOG.ok("Processing trough executeQuery methods for the object class {0}",
                SUBJECT_NAME);

        QueryBuilder queryBuilder = new QueryBuilder(SUBJECT_NAME, filter, columns, TABLE_SU_NAME, operationOptions);
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
            }

        } catch (SQLException e) {

            // TODO
            throw new RuntimeException(e);
        }
    }

    protected boolean handleSqlObject(ResultSet resultSet, ResultsHandler handler) throws SQLException {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();

        ResultSetMetaData meta = resultSet.getMetaData();

        int count = meta.getColumnCount();

        for (int i = 1; i <= count; i++) {
            String name = meta.getColumnName(i);


        }

        return handler.handle(builder.build());
    }
}
