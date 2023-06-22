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
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.sql.*;
import java.util.*;

public class GroupProcessing extends ObjectProcessing {

    private static final Log LOG = Log.getLog(GroupProcessing.class);
    private static final String ATTR_DISPLAY_NAME = "display_name";
    private static final String ATTR_DESCRIPTION = "description";
    private static final String ATTR_ID_IDX = "id_index";
    private static final String TABLE_GR_NAME = "gr_mp_groups";
    private static final String TABLE_GR_EXTENSION_NAME = "gr_mp_group_attributes";
    protected static final String ATTR_UID = ATTR_ID_IDX;
    protected static final String ATTR_NAME = "group_name";
    protected static final String ATTR_MEMBERS = "members";
    protected static final String ATTR_MEMBERS_NATIVE = ATTR_SCT_ID_IDX;
    protected Map<String, Class> columns = new HashMap<>();
    private static final ObjectClass O_CLASS = ObjectClass.GROUP;

    protected Map<String, Class> grMembershipColumns = Map.ofEntries(
            Map.entry(ATTR_GR_ID_IDX, Long.class),
            Map.entry(ATTR_SCT_ID_IDX, Long.class)
    );

    public GroupProcessing(GrouperConfiguration configuration) {

        super(configuration);

        columns.put(ATTR_NAME, String.class);
        columns.put(ATTR_DISPLAY_NAME, String.class);
        columns.put(ATTR_DESCRIPTION, String.class);
        columns.put(ATTR_ID_IDX, Long.class);

        this.columns.putAll(objectColumns);
    }

    @Override
    public void buildObjectClass(SchemaBuilder schemaBuilder, GrouperConfiguration configuration) {
        LOG.info("Building object class definition for {0}", ObjectClass.GROUP_NAME);

        ObjectClassInfoBuilder groupObjClassBuilder = new ObjectClassInfoBuilder();
        groupObjClassBuilder.setType(ObjectClass.GROUP_NAME);

        //subjectObjClassBuilder.addAttributeInfo(OperationalAttributeInfos.ENABLE);
        //subjectObjClassBuilder.addAttributeInfo(OperationalAttributeInfos.PASSWORD);

        //Read-only,

        AttributeInfoBuilder name = new AttributeInfoBuilder(Name.NAME);
        name.setRequired(false).setType(String.class).setCreateable(false).setUpdateable(false).setReadable(true).
                setNativeName(ATTR_NAME);
        groupObjClassBuilder.addAttributeInfo(name.build());


        AttributeInfoBuilder display_name = new AttributeInfoBuilder(ATTR_DISPLAY_NAME);
        display_name.setRequired(false).setType(String.class).setCreateable(false).setUpdateable(false).setReadable(true);
        groupObjClassBuilder.addAttributeInfo(display_name.build());

        AttributeInfoBuilder description = new AttributeInfoBuilder(ATTR_DESCRIPTION);
        description.setRequired(false).setType(String.class).setCreateable(false).setUpdateable(false).setReadable(true);
        groupObjClassBuilder.addAttributeInfo(description.build());

        AttributeInfoBuilder last_modified = new AttributeInfoBuilder(ATTR_MODIFIED);
        last_modified.setRequired(false).setType(Integer.class).setCreateable(false).setUpdateable(false).setReadable(true);
        groupObjClassBuilder.addAttributeInfo(last_modified.build());

        AttributeInfoBuilder deleted = new AttributeInfoBuilder(ATTR_DELETED);
        deleted.setRequired(false).setType(Integer.class).setCreateable(false).setUpdateable(false).setReadable(true);
        groupObjClassBuilder.addAttributeInfo(deleted.build());

        AttributeInfoBuilder members = new AttributeInfoBuilder(ATTR_MEMBERS);
        members.setRequired(false).setType(String.class).setMultiValued(true)
                .setCreateable(false).setUpdateable(false).setReadable(true)
                .setReturnedByDefault(false);
        groupObjClassBuilder.addAttributeInfo(members.build());

        String[] extendedAttrs = configuration.getExtendedGroupProperties();

        if (extendedAttrs != null) {

            List<String> extensionAttrs = Arrays.asList(extendedAttrs);
            for (String attr : extensionAttrs) {

                AttributeInfoBuilder extAttr = new AttributeInfoBuilder(attr);
                extAttr.setRequired(false).setType(String.class).setMultiValued(false)
                        .setCreateable(false).setUpdateable(false).setReadable(true)
                        //TODO should this be returned by default
                        .setReturnedByDefault(false);

                groupObjClassBuilder.addAttributeInfo(extAttr.build());
            }

        }

        schemaBuilder.defineObjectClass(groupObjClassBuilder.build());
    }

    public void executeQuery(Filter filter, ResultsHandler handler, OperationOptions operationOptions
            , Connection connection) {

        LOG.ok("Processing trough executeQuery methods for the object class {0}",
                ObjectClass.GROUP_NAME);

        QueryBuilder queryBuilder;
        if (getAttributesToGet(operationOptions) != null &&
                (getAttributesToGet(operationOptions).contains(ATTR_MEMBERS) && filter != null)) {

            Map<String, Map<String, Class>> tablesAndColumns = new HashMap<>();
            tablesAndColumns.put(TABLE_GR_NAME, columns);
            tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, grMembershipColumns);
            tablesAndColumns.put(TABLE_GR_EXTENSION_NAME, extensionColumns);

            Map<Map<String, String>, String> joinMap = Map.of(
                    Map.of(TABLE_MEMBERSHIP_NAME, ATTR_GR_ID_IDX), ATTR_ID_IDX,
                    Map.of(TABLE_GR_EXTENSION_NAME, ATTR_GR_ID_IDX), ATTR_ID_IDX);

            queryBuilder = new QueryBuilder(O_CLASS, filter,
                    tablesAndColumns, TABLE_GR_NAME, joinMap, operationOptions);
        } else {

            queryBuilder = new QueryBuilder(O_CLASS, filter, Map.of(TABLE_GR_NAME, columns),
                    TABLE_GR_NAME, operationOptions);
        }

        String query = queryBuilder.build();
        ResultSet result = null;
        LOG.info("Query about to be executed: {0}", query);
        try {

            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();
            ConnectorObjectBuilder co = null;

            while (result.next()) {

                LOG.ok(result.toString());

                if (filter instanceof EqualsFilter) {

                    LOG.ok("Processing Equals Query");
                    final EqualsFilter equalsFilter = (EqualsFilter) filter;
                    Attribute fAttr = equalsFilter.getAttribute();
                    if (Uid.NAME.equals(fAttr.getName())) {

                        co = buildConnectorObject(O_CLASS, ATTR_UID, ATTR_NAME, result, operationOptions,
                                columns);

                        // TODO
                        if (getAttributesToGet(operationOptions) != null &&
                                getAttributesToGet(operationOptions).contains(ATTR_MEMBERS)) {

                            populateMembershipAttribute(result, co, configuration);
                        }
                        handler.handle(co.build());
                        break;

                    } else {

                        co = buildConnectorObject(O_CLASS, ATTR_UID, ATTR_NAME, result, operationOptions,
                                columns);
                        handler.handle(co.build());
                    }
                } else {

                    co = buildConnectorObject(O_CLASS, ATTR_UID, ATTR_NAME, result, operationOptions,
                            columns);
                    handler.handle(co.build());
                }
            }

        } catch (SQLException e) {

            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ConnectorObjectBuilder populateMembershipAttribute(ResultSet result, ConnectorObjectBuilder ob,
                                                                 GrouperConfiguration configuration)
            throws SQLException {

        HashMap<String, Set<Object>> multiValues = new HashMap<>();

        buildMultiValued(result, Collections.singletonMap(ATTR_SCT_ID_IDX, String.class), multiValues,
                configuration);
        while (result.next()) {

            buildMultiValued(result, Collections.singletonMap(ATTR_SCT_ID_IDX, String.class), multiValues,
                    configuration);
        }

        for (String attrName : multiValues.keySet()) {
            LOG.ok("Adding attribute values for the attribute {0} to the attribute builder.", attrName);

            if (ATTR_SCT_ID_IDX.equals(attrName)) {

                ob.addAttribute(ATTR_MEMBERS, multiValues.get(attrName));
            } else {

                ob.addAttribute(attrName, multiValues.get(attrName));
            }
        }

        return ob;
    }

    private void buildMultiValued(ResultSet result, Map<String, Class> columns,
                                  HashMap<String, Set<Object>> multiValues, GrouperConfiguration configuration)
            throws SQLException {

        LOG.info("Evaluation of SQL objects present in result set for multivalued attributes.");

        ResultSetMetaData meta = result.getMetaData();

        String extAttrName = null;
        String etxAttrValue = null;

        String[] groupImProperties = configuration.getExtendedGroupProperties();

        List<String> extGroupProperties = null;

        if (groupImProperties != null && groupImProperties.length > 0) {

            extGroupProperties = List.of(groupImProperties);
        }

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

                    Long resVal = result.getLong(i);

                    attrValues.add(result.wasNull() ? null : Long.toString(resVal));
                    multiValues.put(name, attrValues);
                }

                if (type.equals(String.class)) {

                    LOG.ok("Addition of String type attribute for attribute from column with name {0} to the multivalued" +
                            " collection", name);

                    attrValues.add(result.getString(i));
                    multiValues.put(name, attrValues);
                }

            } else {

                if (ATTR_EXT_NAME.equals(name)) {

                    extAttrName = result.getString(i);
                    LOG.ok("Processing ext attr name: {0}", extAttrName);
                } else if (ATTR_EXT_VALUE.equals(name)) {

                    etxAttrValue = result.getString(i);
                    LOG.ok("Processing ext attr val: {0}", etxAttrValue);
                } else {

                    LOG.info("SQL object handling discovered during multivalued attribute evaluation a " +
                            "column which is not present in the original schema set. The column name: {0}", name);
                }
            }
        }

        LOG.ok("Not present in TYPE map: {0}", extAttrName);

        if (extGroupProperties != null) {

            if (extAttrName != null && extGroupProperties.contains(extAttrName)) {

                Set<Object> extSet = new HashSet<>();
                extSet.add(etxAttrValue);
                multiValues.put(extAttrName, extSet);
            } else {

                if (extAttrName != null) {
                } else {

                    LOG.info("Attribute with the name: {0}, not present in the resource schema.", extAttrName);
                }
            }
        }

    }

    public Set<String> fetchExtensionSchema(Connection connection) throws SQLException {

        ResultSet result = null;
        QueryBuilder queryBuilder = new QueryBuilder(O_CLASS, TABLE_GR_EXTENSION_NAME, 1000);
        String query = queryBuilder.build();

        PreparedStatement prepareStatement = connection.prepareStatement(query);
        result = prepareStatement.executeQuery();


        Set<String> extensionAttributeNames = new HashSet<>();
        while (result.next()) {


            ResultSetMetaData meta = result.getMetaData();

            int count = meta.getColumnCount();
            LOG.ok("Number of columns returned from result set object: {0}", count);
            // options

            for (int i = 1; i <= count; i++) {
                String name = meta.getColumnName(i);

                if (ATTR_EXT_NAME.equals(name)) {
                    String nameValue = result.getString(i);

                    LOG.ok("Extension attribute name which is being added to extended resource schema: {0}", nameValue);
                    extensionAttributeNames.add(result.getString(i));
                }

                //TODO
                LOG.ok("# Column name {0}", name);
            }

        }

        return extensionAttributeNames;
    }
}
