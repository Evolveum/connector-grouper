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
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.framework.common.objects.filter.GreaterThanFilter;

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

    protected Set<String> multiValuedAttributesCatalogue = new HashSet();
    protected Map<String, Class> columns = new HashMap<>();
    private static final ObjectClass O_CLASS = ObjectClass.GROUP;

    protected Map<String, Class> grMembershipColumns = Map.ofEntries(
            Map.entry(ATTR_GR_ID_IDX, Long.class),
            Map.entry(ATTR_SCT_ID_IDX, Long.class)
    );

    protected Map<String, Class> objectConstructionSchema = Map.ofEntries(
            Map.entry(ATTR_SCT_ID_IDX, Long.class),
            Map.entry(ATTR_NAME, String.class),
            Map.entry(ATTR_DISPLAY_NAME, String.class),
            Map.entry(ATTR_DESCRIPTION, String.class),
            Map.entry(ATTR_ID_IDX, String.class),
            Map.entry(ATTR_DELETED, String.class),
            Map.entry(ATTR_EXT_NAME, String.class),
            Map.entry(ATTR_EXT_VALUE, String.class)
    );


    public GroupProcessing(GrouperConfiguration configuration) {

        super(configuration);

        columns.put(ATTR_NAME, String.class);
        columns.put(ATTR_DISPLAY_NAME, String.class);
        columns.put(ATTR_DESCRIPTION, String.class);
        columns.put(ATTR_ID_IDX, Long.class);

        this.columns.putAll(objectColumns);

        multiValuedAttributesCatalogue.add(ATTR_MEMBERS);
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
                        //TODO should this be returned by default ?
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
        List<String> extended = configuration.getExtendedGroupProperties() != null ?
                Arrays.asList(configuration.getExtendedGroupProperties()) : null;

        if (getAttributesToGet(operationOptions) != null &&
                (!getAttributesToGet(operationOptions).isEmpty() && filter != null)) {

            Map<String, Map<String, Class>> tablesAndColumns = new HashMap<>();
            Map<Map<String, String>, String> joinMap = new HashMap<>();

            tablesAndColumns.put(TABLE_GR_NAME, columns);

            if (getAttributesToGet(operationOptions).contains(ATTR_MEMBERS)) {

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, membershipColumns);
                joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_GR_ID_IDX), ATTR_ID_IDX);
            }

            if (getAttributesToGet(operationOptions).stream().anyMatch(atg -> extended.contains(atg))) {

                tablesAndColumns.put(TABLE_GR_EXTENSION_NAME, extensionColumns);
                joinMap.put(Map.of(TABLE_GR_EXTENSION_NAME, ATTR_GR_ID_IDX), ATTR_ID_IDX);
            }

            queryBuilder = new QueryBuilder(O_CLASS, filter,
                    tablesAndColumns, TABLE_GR_NAME, joinMap, operationOptions);
        } else {

            queryBuilder = new QueryBuilder(O_CLASS, filter, Map.of(TABLE_GR_NAME, columns),
                    TABLE_GR_NAME, operationOptions);
        }

        queryBuilder.setUseFullAlias(true);
        String query = queryBuilder.build();
        ResultSet result = null;

        LOG.info("Query about to be executed: {0}", query);

        Map<String, GrouperObject> objects = new HashMap<>();
        try {

            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();

            while (result.next()) {


                Map<String, Class> mergedColumns = new HashMap<>();
                mergedColumns.putAll(columns);
                mergedColumns.putAll(grMembershipColumns);
                mergedColumns.putAll(extensionColumns);

                GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_NAME, result, objectConstructionSchema,
                        multiValuedAttributesCatalogue, Map.of(ATTR_MEMBERS_NATIVE, ATTR_MEMBERS));

                if (objects.isEmpty()) {
                    objects.put(go.getIdentifier(), go);

                } else {
                    String objectID = go.getIdentifier();

                    if (objects.containsKey(objectID)) {

                        GrouperObject mapObject = objects.get(objectID);

                        Map<String, Object> attrMap = go.getAttributes();

                        for (String attName : attrMap.keySet()) {

                            mapObject.addAttribute(attName, attrMap.get(attName), multiValuedAttributesCatalogue);

                        }

                    } else {

                        objects.put(go.getIdentifier(), go);
                    }
                }

            }

            if (objects.isEmpty()) {
                LOG.ok("Empty object set execute query");
            } else {
                for (String objectName : objects.keySet()) {

                    LOG.info("The object name: {0}", objectName);

                    LOG.info("The object: {0}", objects.get(objectName).toString());

                    GrouperObject go = objects.get(objectName);
                    if (configuration.getExcludeDeletedObjects()) {
                        if (go.isDeleted()) {
                            LOG.ok("Following object omitted from evaluation, because it's deleted" +
                                    "identifier: {0} ; name: {1}.", go.getIdentifier(), go.getName());

                            continue;
                        }
                    }

                    ConnectorObjectBuilder co = buildConnectorObject(O_CLASS, go, operationOptions);
                    if (!handler.handle(co.build())) {

                        LOG.warn("Result handling interrupted by handler!");
                        break;
                    }

                }
            }
        } catch (SQLException e) {

            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getMemberShipAttributeName() {
        return ATTR_MEMBERS;
    }

    @Override
    protected String getExtensionAttributeTableName() {
        return TABLE_GR_EXTENSION_NAME;
    }

    @Override
    protected String getMembershipTableName() {
        return TABLE_MEMBERSHIP_NAME;
    }

    @Override
    protected String getMainTableName() {
        return TABLE_GR_NAME;
    }

    @Override
    public void sync(SyncToken syncToken, SyncResultsHandler syncResultsHandler, OperationOptions operationOptions,
                     Connection connection) {
        QueryBuilder queryBuilder;

        String tokenVal;
        if (syncToken.getValue() instanceof Long) {

            tokenVal = Long.toString((Long) syncToken.getValue());
        } else {
            tokenVal = (String) syncToken.getValue();
        }

        LOG.ok("The sync token value in the evaluation of subject processing sync method: {0}", tokenVal);

        GreaterThanFilter greaterThanFilterBase = (GreaterThanFilter)
                FilterBuilder.greaterThan(AttributeBuilder.build(TABLE_GR_NAME + "." + ATTR_MODIFIED,
                        tokenVal));

        GreaterThanFilter greaterThanFilterMember = null;

        GreaterThanFilter greaterThanFilterExtension = null;

        Filter filter = greaterThanFilterBase;

        List<String> extended = configuration.getExtendedSubjectProperties() != null ?
                Arrays.asList(configuration.getExtendedSubjectProperties()) : null;

        if (getAttributesToGet(operationOptions) != null &&
                !getAttributesToGet(operationOptions).isEmpty()) {


            Map<String, Map<String, Class>> tablesAndColumns = new HashMap<>();
            Map<Map<String, String>, String> joinMap = new HashMap<>();

            tablesAndColumns.put(TABLE_GR_NAME, Map.of(ATTR_DELETED, String.class,
                    ATTR_ID_IDX, Long.class, ATTR_MODIFIED, Long.class));

            Set<String> attrsToGet = getAttributesToGet(operationOptions);


            if (attrsToGet.contains(ATTR_MEMBERS)) {

                greaterThanFilterMember = (GreaterThanFilter)
                        FilterBuilder.greaterThan(AttributeBuilder.build(TABLE_MEMBERSHIP_NAME + "." + ATTR_MODIFIED,
                                tokenVal));

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, objectColumns);
                joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
            }

            if (attrsToGet.stream().anyMatch(atg -> extended.contains(atg))) {

                greaterThanFilterExtension = (GreaterThanFilter)
                        FilterBuilder.greaterThan(AttributeBuilder.build(TABLE_GR_EXTENSION_NAME + "." +
                                        ATTR_MODIFIED,
                                tokenVal));

                tablesAndColumns.put(TABLE_GR_EXTENSION_NAME, objectColumns);
                joinMap.put(Map.of(TABLE_GR_EXTENSION_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
            }

            if (greaterThanFilterMember != null && greaterThanFilterExtension != null) {

                filter = FilterBuilder.or(greaterThanFilterMember, greaterThanFilterBase,
                        greaterThanFilterExtension);
            } else if (greaterThanFilterMember != null) {

                filter = FilterBuilder.or(greaterThanFilterMember, greaterThanFilterBase);
            } else if (greaterThanFilterExtension != null) {

                filter = FilterBuilder.or(greaterThanFilterBase,
                        greaterThanFilterExtension);
            }

            queryBuilder = new QueryBuilder(O_CLASS, filter,
                    tablesAndColumns, TABLE_GR_NAME, joinMap, operationOptions);
        } else {

            queryBuilder = new QueryBuilder(O_CLASS, filter, Map.of(TABLE_GR_NAME, columns),
                    TABLE_GR_NAME, operationOptions);
        }
        queryBuilder.setUseFullAlias(true);
        queryBuilder.setOrderByASC(CollectionUtil.newSet(ATTR_MODIFIED_LATEST));
        queryBuilder.setAsSyncQuery(true);

        String query = queryBuilder.build();

        ResultSet result = null;

        Map<String, GrouperObject> objects = new LinkedHashMap<>();
        try {
            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();

            while (result.next()) {

                Map<String, Class> mergedColumns = new HashMap<>();
                mergedColumns.putAll(columns);
                mergedColumns.putAll(grMembershipColumns);
                mergedColumns.putAll(extensionColumns);

                GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_NAME, result, objectConstructionSchema,
                        multiValuedAttributesCatalogue, null);

                if (objects.isEmpty()) {
                    objects.put(go.getIdentifier(), go);

                } else {
                    String objectID = go.getIdentifier();

                    if (objects.containsKey(objectID)) {

                        GrouperObject mapObject = objects.get(objectID);

                        Map<String, Object> attrMap = go.getAttributes();

                        for (String attName : attrMap.keySet()) {

                            mapObject.addAttribute(attName, attrMap.get(attName), multiValuedAttributesCatalogue);

                        }

                    } else {

                        objects.put(go.getIdentifier(), go);
                    }
                }

            }

            if (objects.isEmpty()) {
                LOG.ok("Empty object set in sync op");
            } else {

                Map<String, GrouperObject> notDeletedObject = new LinkedHashMap<>();

                for (String id : objects.keySet()) {
                    GrouperObject object = objects.get(id);

                    if (object.isDeleted()) {

                        LOG.ok("### {0} is deleted", id);
                    } else {

                        notDeletedObject.put(id, object);
                        LOG.ok("### {0}", id);
                    }
                }

                if (!notDeletedObject.isEmpty()) {
                    notDeletedObject = fetchFullNonDeletedObjects(notDeletedObject, operationOptions, connection);
                }

                for (String id : objects.keySet()) {

                    SyncDeltaBuilder builder = new SyncDeltaBuilder();
                    builder.setObjectClass(O_CLASS);
                    GrouperObject objectPartial = objects.get(id);
                    if (!notDeletedObject.isEmpty() && notDeletedObject.containsKey(id)) {

                        GrouperObject nonDelObjFull = notDeletedObject.get(id);
                        builder.setDeltaType(SyncDeltaType.CREATE_OR_UPDATE);
                        builder.setUid(new Uid(id));
                        builder.setToken(new SyncToken(objectPartial.getLatestTimestamp()));

                        ConnectorObjectBuilder objectBuilder = buildConnectorObject(O_CLASS, nonDelObjFull,
                                operationOptions);

                        builder.setObject(objectBuilder.build());


                    } else {

                        builder.setDeltaType(SyncDeltaType.DELETE);
                        LOG.ok("### {0} is deleted", id);
                        builder.setUid(new Uid(id));
                        builder.setToken(new SyncToken(objectPartial.getLatestTimestamp()));

                    }
                    SyncDelta syncdelta = builder.build();

                    if (!syncResultsHandler.handle(syncdelta)) {

                        LOG.warn("Result handling interrupted by handler!");
                        break;
                    }
                }
            }

        } catch (SQLException e) {
            //TODO
            throw new RuntimeException(e);
        }

    }

    @Override
    public SyncToken getLatestSyncToken(Connection connection) {
        LOG.ok("Processing through the 'getLatestSyncToken' method for the objectClass {0}", ObjectClass.GROUP);

        Map<String, Map<String, Class>> tablesAndColumns = new HashMap<>();
        Map<Map<String, String>, String> joinMap = new HashMap<>();

        // Joining all tables related to object type
        tablesAndColumns.put(TABLE_GR_NAME, Map.of(ATTR_MODIFIED, Long.class));
        tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, Map.of(ATTR_MODIFIED, Long.class));
        tablesAndColumns.put(TABLE_GR_EXTENSION_NAME, Map.of(ATTR_MODIFIED, Long.class));

        joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_GR_ID_IDX), ATTR_ID_IDX);
        joinMap.put(Map.of(TABLE_GR_EXTENSION_NAME, ATTR_GR_ID_IDX), ATTR_ID_IDX);


        QueryBuilder queryBuilder = new QueryBuilder(O_CLASS, null,
                tablesAndColumns, TABLE_GR_NAME, joinMap, null);
        queryBuilder.setOrderByASC(CollectionUtil.newSet(ATTR_MODIFIED_LATEST));

        String query = queryBuilder.buildSyncTokenQuery();

        ResultSet result = null;
        try {
            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();

            while (result.next()) {

                ResultSetMetaData meta = result.getMetaData();
                int count = meta.getColumnCount();

                for (int i = 1; i <= count; i++) {
                    String name = meta.getColumnName(i);
                    LOG.ok("Evaluation of column with name {0}", name);
                    Long resVal = result.getLong(i);

                    Long val = result.wasNull() ? null : resVal;

                    return new SyncToken(val);
                }
            }

        } catch (SQLException e) {
            //TODO
            throw new RuntimeException(e);
        }

        throw new ConnectorException("Latest sync token could not be fetched.");
    }

    private Map<String, GrouperObject> fetchFullNonDeletedObjects(Map<String, GrouperObject> notDeletedObject,
                                                                  OperationOptions operationOptions,
                                                                  Connection connection) {

        QueryBuilder queryBuilder;

        Set<String> idSet = new LinkedHashSet<>();
        for (String identifier : notDeletedObject.keySet()) {

            idSet.add(identifier);
        }

        List<String> extended = configuration.getExtendedGroupProperties() != null ?
                Arrays.asList(configuration.getExtendedGroupProperties()) : null;

        if (getAttributesToGet(operationOptions) != null &&
                !getAttributesToGet(operationOptions).isEmpty()) {


            Map<String, Map<String, Class>> tablesAndColumns = new HashMap<>();
            Map<Map<String, String>, String> joinMap = new HashMap<>();

            tablesAndColumns.put(TABLE_GR_NAME, columns);

            Set<String> attrsToGet = getAttributesToGet(operationOptions);


            if (attrsToGet.contains(ATTR_MEMBERS)) {

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, membershipColumns);
                joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_GR_ID_IDX), ATTR_ID_IDX);
            }

            if (attrsToGet.stream().anyMatch(atg -> extended.contains(atg))) {

                tablesAndColumns.put(TABLE_GR_EXTENSION_NAME, extensionColumns);
                joinMap.put(Map.of(TABLE_GR_EXTENSION_NAME, ATTR_GR_ID_IDX), ATTR_ID_IDX);
            }

            queryBuilder = new QueryBuilder(O_CLASS, null,
                    tablesAndColumns, TABLE_GR_NAME, joinMap, operationOptions);
        } else {

            queryBuilder = new QueryBuilder(O_CLASS, null, Map.of(TABLE_GR_NAME, columns),
                    TABLE_GR_NAME, operationOptions);
        }

        queryBuilder.setUseFullAlias(true);
        queryBuilder.setInStatement(Map.of(TABLE_GR_NAME + "." + ATTR_UID, idSet));

        String query = queryBuilder.build();

        ResultSet result = null;

        Map<String, GrouperObject> objects = new HashMap<>();

        try {

            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();

            while (result.next()) {

                Map<String, Class> mergedColumns = new HashMap<>();
                mergedColumns.putAll(columns);
                mergedColumns.putAll(grMembershipColumns);
                mergedColumns.putAll(extensionColumns);


                GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_NAME, result, objectConstructionSchema,
                        multiValuedAttributesCatalogue, Map.of(ATTR_MEMBERS_NATIVE, ATTR_MEMBERS));

                if (objects.isEmpty()) {
                    objects.put(go.getIdentifier(), go);

                } else {
                    String objectID = go.getIdentifier();

                    if (objects.containsKey(objectID)) {

                        GrouperObject mapObject = objects.get(objectID);

                        Map<String, Object> attrMap = go.getAttributes();

                        for (String attName : attrMap.keySet()) {

                            mapObject.addAttribute(attName, attrMap.get(attName), multiValuedAttributesCatalogue);

                        }

                    } else {

                        objects.put(go.getIdentifier(), go);
                    }
                }

            }

            if (objects.isEmpty()) {
                LOG.ok("Empty object set in sync op");
            } else {
                for (String objectName : objects.keySet()) {

                    LOG.info("The object name: {0}", objectName);

                    LOG.info("The object: {0}", objects.get(objectName).toString());
                }
            }

            return objects;
        } catch (SQLException e) {
            //TODO
            throw new RuntimeException(e);
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
            }

        }

        return extensionAttributeNames;
    }
}
