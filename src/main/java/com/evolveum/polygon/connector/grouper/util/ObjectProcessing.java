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
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public abstract class ObjectProcessing {
    private static final Log LOG = Log.getLog(ObjectProcessing.class);

public static final String SUBJECT_NAME = "Subject";
    protected static final String ATTR_MODIFIED = "last_modified";
    protected static final String TABLE_MEMBERSHIP_NAME = "gr_mp_memberships";
    protected static final String ATTR_GR_ID_IDX = "group_id_index";
    protected static final String ATTR_SCT_ID_IDX = "subject_id_index";

    protected static final String ATTR_DELETED = "deleted";

    protected Map<String, Class> objectColumns = Map.ofEntries(
            Map.entry(ATTR_MODIFIED, Long.class),
            Map.entry(ATTR_DELETED, String.class)
    );

    protected Map<String, Class> membershipColumns = Map.ofEntries(
            Map.entry(ATTR_GR_ID_IDX, String.class),
            Map.entry(ATTR_SCT_ID_IDX, String.class),
            Map.entry(ATTR_MODIFIED, Long.class),
            Map.entry(ATTR_DELETED, String.class)
    );


    public abstract void buildObjectClass(SchemaBuilder schemaBuilder);

    public abstract void executeQuery(Filter filter, ResultsHandler handler, OperationOptions operationOptions
            , Connection connection);


    protected ConnectorObjectBuilder buildConnectorObject(ObjectClass o_class, String uid_name, String name_name,
                                                          ResultSet resultSet, OperationOptions oo,
                                                          Map<String, Class> columns) throws SQLException {
        return buildConnectorObject(o_class, uid_name, name_name, resultSet, oo, columns, null);
    }

    protected ConnectorObjectBuilder buildConnectorObject(ObjectClass o_class, String uid_name, String name_name,
                                                          ResultSet resultSet, OperationOptions oo,
                                                          Map<String, Class> columns, ConnectorObjectBuilder ob)
            throws SQLException {

        LOG.info("Evaluation of SQL objects present in result set.");

        ConnectorObjectBuilder builder;

        if (ob != null) {

            builder = ob;
        } else if (o_class != null) {

            builder = new ConnectorObjectBuilder();
            builder.setObjectClass(o_class);
        } else {

            throw new ConnectorException("Unexpected exception while building connector object, object builder nor " +
                    "object class present in method invocation.");
        }


        ResultSetMetaData meta = resultSet.getMetaData();

        int count = meta.getColumnCount();
        LOG.ok("Number of columns returned from result set object: {0}", count);
        // TODO Based on options the handling might be paginated
        // options

        for (int i = 1; i <= count; i++) {
            String name = meta.getColumnName(i);
            LOG.ok("Evaluation of column with name {0}", name);

            if (uid_name != null && name.equals(uid_name)) {

                String uidVal = Long.toString(resultSet.getLong(i));
                LOG.ok("Addition of UID attribute {0}, the value {1}", uid_name, uidVal);

                builder.setUid(new Uid(uidVal));

            } else if (name_name != null && name.equals(name_name)) {

                String nameVal = resultSet.getString(i);
                LOG.ok("Addition of Name attribute {0}, the value {1}", name_name, nameVal);

                builder.setName(nameVal);
            } else {

                if (columns.containsKey(name)) {
                    Class type = columns.get(name);

                    if (type.equals(Long.class)) {

                        LOG.ok("Addition of Long type attribute for attribute from column with name {0}", name);


                        Long resVal = resultSet.getLong(i);


                        if (name.equals(ATTR_MODIFIED)) {

                            builder.addAttribute(name, resultSet.wasNull() ? null : resVal);
                        } else {

                            builder.addAttribute(name, resultSet.wasNull() ? null : Long.toString(resVal));
                        }
                    }

                    if (type.equals(String.class)) {

                        LOG.ok("Addition of String type attribute for attribute from column with name {0}", name);
                        builder.addAttribute(name, resultSet.getString(i));
                    }

                } else {

                    LOG.info("SQL object handling discovered a column which is not present in the original schema set. " +
                            "The column name: {0}", name);
                }
            }
        }

        LOG.ok("Returning builder");
        return builder;
    }

    protected abstract ConnectorObjectBuilder populateMembershipAttribute(ResultSet result, ConnectorObjectBuilder ob)
            throws SQLException;

    protected Set<String> getAttributesToGet(OperationOptions operationOptions) {
        if (operationOptions != null || operationOptions.getAttributesToGet() != null) {

            return new HashSet<>(Arrays.asList(operationOptions.getAttributesToGet()));
        }

        return null;
    }
}
