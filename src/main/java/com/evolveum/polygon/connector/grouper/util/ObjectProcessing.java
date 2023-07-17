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

import com.evolveum.polygon.connector.grouper.GrouperConfiguration;
import org.identityconnectors.common.logging.Log;
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
    protected static final String ATTR_EXT_NAME = "attribute_name";
    protected static final String ATTR_EXT_VALUE = "attribute_value";
    protected static final String ATTR_DELETED = "deleted";
    protected static final String ATTR_DELETED_TRUE = "T";
    protected static final String ATTR_MODIFIED_LATEST = "latest_timestamp";
    protected GrouperConfiguration configuration;

    protected Map<String, Class> objectColumns = Map.ofEntries(
            Map.entry(ATTR_MODIFIED, Long.class),
            Map.entry(ATTR_DELETED, String.class)
    );

    protected Map<String, Class> extensionColumns = Map.ofEntries(
            Map.entry(ATTR_EXT_NAME, String.class),
            Map.entry(ATTR_EXT_VALUE, String.class),
            //TODO test
            Map.entry(ATTR_MODIFIED, Long.class),
            Map.entry(ATTR_DELETED, String.class)
    );

    protected Map<String, Class> membershipColumns = Map.ofEntries(
            Map.entry(ATTR_GR_ID_IDX, String.class),
            Map.entry(ATTR_SCT_ID_IDX, String.class),
            Map.entry(ATTR_MODIFIED, Long.class),
            Map.entry(ATTR_DELETED, String.class)
    );

    public ObjectProcessing(GrouperConfiguration configuration) {

        this.configuration = configuration;
    }

    public abstract void buildObjectClass(SchemaBuilder schemaBuilder, GrouperConfiguration configuration);

    public abstract void executeQuery(Filter filter, ResultsHandler handler, OperationOptions operationOptions
            , Connection connection);


    protected GrouperObject buildGrouperObject(String uid_name, String name_name,
                                               ResultSet resultSet,
                                               Map<String, Class> columns, Set<String> multiValuedAttributesCatalogue, Map<String, String> renameSet)
            throws SQLException {
        return buildGrouperObject(uid_name, name_name, resultSet, columns, null, multiValuedAttributesCatalogue, renameSet);
    }

    protected GrouperObject buildGrouperObject(String uid_name, String name_name,
                                               ResultSet resultSet,
                                               Map<String, Class> columns, GrouperObject ob,
                                               Set<String> multiValuedAttributesCatalogue,
                                               Map<String, String> renameSet)
            throws SQLException {

        LOG.info("Evaluation of SQL objects present in result set.");

        GrouperObject grouperObject;
        String extAttrName = null;
        String etxAttrValue = null;
        String membershipColumnValue = null;

        Boolean saturateMembership = true;
        Boolean saturateExtensionAttribute = true;

        if (ob != null) {
            grouperObject = ob;

        } else {
            grouperObject = new GrouperObject();
//            throw new ConnectorException("Unexpected exception while building object, object builder nor " +
//                    "object class present in method invocation.");
        }


        ResultSetMetaData meta = resultSet.getMetaData();

        int count = meta.getColumnCount();
        LOG.ok("Number of columns returned from result set object: {0}", count);
        // TODO Based on options the handling might be paginated
        // options

        for (int i = 1; i <= count; i++) {
            String name = meta.getColumnName(i);
            String origName = name;
            String tableName = null;

            if (name.contains("$")) {

                String[] nameParts = name.split("\\$");
                tableName = nameParts[0];
                name = nameParts[1];

            }

            LOG.ok("Evaluation of column with name {0}", name);

            if (uid_name != null && name.equals(uid_name)) {

                String uidVal = Long.toString(resultSet.getLong(i));
                LOG.ok("Addition of UID attribute {0}, the value {1}", uid_name, uidVal);

                grouperObject.setIdentifier(uidVal);
            } else if (name_name != null && name.equals(name_name)) {

                String nameVal = resultSet.getString(i);
                LOG.ok("Addition of Name attribute {0}, the value {1}", name_name, nameVal);

                //TODO
                grouperObject.setName(nameVal);
            } else if (ATTR_EXT_NAME.equals(name)) {

                extAttrName = resultSet.getString(i);
                LOG.ok("Addition of Name attribute {0}", extAttrName);

                //TODO
            } else if (ATTR_EXT_VALUE.equals(name)) {

                etxAttrValue = resultSet.getString(i);
                LOG.ok("Addition of extension value attribute, the value {0}", etxAttrValue);
            } else if (ATTR_DELETED.equals(name)) {

                String deleted = resultSet.getString(i);

                if (getMainTableName().equals(tableName)) {

                    if (deleted != null && ATTR_DELETED_TRUE.equals(deleted)) {

                        grouperObject.setDeleted(true);
                        LOG.info("Object" + name_name != null ? " " + name_name + " vas set to deleted" : "vas set to" +
                                " deleted");
                    }

                } else if (getMembershipTableName().equals(tableName)) {

                    if (deleted != null && ATTR_DELETED_TRUE.equals(deleted)) {

                        saturateMembership = false;

                        LOG.info("Object" + name_name != null ? " " + name_name + " membership row was marked as" +
                                " deleted" : "membership column was marked as deleted");
                    }

                } else if (getExtensionAttributeTableName().equals(tableName)) {

                    if (deleted != null && ATTR_DELETED_TRUE.equals(deleted)) {

                        saturateExtensionAttribute = false;

                        LOG.info("Object" + name_name != null ? " " + name_name + " extension attribute row was marked as" +
                                " deleted" : "membership column was marked as deleted");
                    }

                }

            } else if (ATTR_MODIFIED_LATEST.equals(name)) {

                Long timestamp_latest = resultSet.getLong(i);

                if (timestamp_latest != null) {

                    grouperObject.setLatestTimestamp(timestamp_latest);
                }

                LOG.info("Object" + name_name != null ? " " + name_name + " vas set to deleted" : "vas set to deleted");
            } else {

                if (columns.containsKey(name)) {
                    Class type = columns.get(name);

                    if (renameSet != null && !renameSet.isEmpty()) {
                        if (renameSet.containsKey(name)) {
                            name = renameSet.get(name);
                        }
                    }

                    if (type.equals(Long.class)) {

                        LOG.ok("Addition of Long type attribute for attribute from column with name {0}", origName);


                        Long resVal = resultSet.getLong(i);


                        if (name.equals(ATTR_MODIFIED)) {

                            grouperObject.addAttribute(name, resultSet.wasNull() ? null : resVal,
                                    multiValuedAttributesCatalogue);

                        } else {

                            if (getMemberShipAttributeName().equals(name)) {

                                membershipColumnValue = resultSet.wasNull() ? null : Long.toString(resVal);
                            } else {

                                grouperObject.addAttribute(name, resultSet.wasNull() ? null : Long.toString(resVal),
                                        multiValuedAttributesCatalogue);
                            }
                        }
                    }

                    if (type.equals(String.class)) {

                        LOG.ok("Addition of String type attribute for attribute from column with name {0}", name);

                        grouperObject.addAttribute(name, resultSet.getString(i),
                                multiValuedAttributesCatalogue);
                    }

                } else {

                    LOG.info("SQL object handling discovered a column which is not present in the " +
                            "original schema set. The column name: {0}", name);
                }
            }
        }

        if (extAttrName != null) {
            if (configuration.getExcludeDeletedObjects()) {


                if (saturateExtensionAttribute) {

                    grouperObject.addAttribute(extAttrName, etxAttrValue, multiValuedAttributesCatalogue);
                }
            } else {

                grouperObject.addAttribute(extAttrName, etxAttrValue, multiValuedAttributesCatalogue);
            }
        }

        if (membershipColumnValue != null) {

            if (configuration.getExcludeDeletedObjects()) {
                if (saturateMembership) {
                    grouperObject.addAttribute(getMemberShipAttributeName(), membershipColumnValue,
                            multiValuedAttributesCatalogue);
                }
            } else {

                grouperObject.addAttribute(extAttrName, etxAttrValue, multiValuedAttributesCatalogue);
            }
        }

        return grouperObject;
    }

    protected abstract String getMemberShipAttributeName();

    protected abstract String getExtensionAttributeTableName();

    protected abstract String getMembershipTableName();

    protected abstract String getMainTableName();

    protected ConnectorObjectBuilder buildConnectorObject(ObjectClass o_class, GrouperObject grouperObject,
                                                          OperationOptions oo) {

        LOG.ok("Processing trough the buildConnectorObject method for grouper object {0}, of object class {1}",
                grouperObject.getIdentifier(), o_class);
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
        builder.setObjectClass(o_class);

        builder.setUid(new Uid(grouperObject.getIdentifier()));
        builder.setName(grouperObject.getName());

        Map<String, Object> attrs = grouperObject.getAttributes();

        for (String name : attrs.keySet()) {

            if (attrs.get(name) instanceof HashSet<?>) {
                builder.addAttribute(name, (Set) attrs.get(name));
            } else {

                builder.addAttribute(name, attrs.get(name));
            }
        }

        return builder;
    }

    protected abstract GrouperObject populateOptionalAttributes(ResultSet result, GrouperObject ob,
                                                                GrouperConfiguration configuration)
            throws SQLException;

    protected Set<String> getAttributesToGet(OperationOptions operationOptions) {
        if (operationOptions != null && operationOptions.getAttributesToGet() != null) {

            return new HashSet<>(Arrays.asList(operationOptions.getAttributesToGet()));
        }

        return null;
    }

    protected abstract void sync(SyncToken syncToken, SyncResultsHandler syncResultsHandler,
                                 OperationOptions operationOptions, Connection connection);


    public abstract SyncToken getLatestSyncToken(Connection connection);
}
