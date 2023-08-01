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
import org.identityconnectors.framework.common.objects.filter.*;

import java.sql.*;
import java.util.*;

public class SubjectProcessing extends ObjectProcessing {

    private static final Log LOG = Log.getLog(SubjectProcessing.class);
    private static final String ATTR_ID = "subject_id";
    private static final String ATTR_ID_IDX = "subject_id_index";
    private static final String TABLE_SU_NAME = "gr_mp_subjects";
    private static final String TABLE_SU_EXTENSION_NAME = "gr_mp_subject_attributes";
    public static final ObjectClass O_CLASS = new ObjectClass(SUBJECT_NAME);
    protected static final String ATTR_UID = ATTR_ID_IDX;
    protected static final String ATTR_NAME = ATTR_ID;
    protected static final String ATTR_MEMBER_OF = "member_of";
    protected static final String ATTR_MEMBER_OF_NATIVE = ATTR_GR_ID_IDX;

    protected Set<String> multiValuedAttributesCatalogue = new HashSet();
    protected Map<String, Class> columns = new HashMap<>();
    protected Map<String, Class> suMembershipColumns = Map.ofEntries(
            Map.entry(ATTR_GR_ID_IDX, Long.class),

            Map.entry(ATTR_MODIFIED, Long.class)

    );

    protected Map<String, Class> objectConstructionSchema = Map.ofEntries(
            Map.entry(ATTR_GR_ID_IDX, Long.class),
            Map.entry(ATTR_NAME, String.class),
            Map.entry(ATTR_ID_IDX, String.class),
            Map.entry(ATTR_DELETED, String.class),
            Map.entry(ATTR_EXT_NAME, String.class),
            Map.entry(ATTR_EXT_VALUE, String.class)
    );

    public SubjectProcessing(GrouperConfiguration configuration) {

        super(configuration);

        columns.put(ATTR_ID_IDX, Long.class);
        columns.put(ATTR_ID, String.class);
        this.columns.putAll(objectColumns);

        multiValuedAttributesCatalogue.add(ATTR_MEMBER_OF);
    }

    @Override
    public void buildObjectClass(SchemaBuilder schemaBuilder, GrouperConfiguration configuration) {
        LOG.info("Building object class definition for {0}", SUBJECT_NAME);

        ObjectClass objectClass = new ObjectClass(SUBJECT_NAME);
        ObjectClassInfoBuilder subjectObjClassBuilder = new ObjectClassInfoBuilder();
        subjectObjClassBuilder.setType(objectClass.getObjectClassValue());

        //Read-only,
        AttributeInfoBuilder id = new AttributeInfoBuilder(Name.NAME);
        id.setRequired(true).setType(String.class).setCreateable(false).setUpdateable(false).setReadable(true)
                .setNativeName(ATTR_ID);
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
                .setReturnedByDefault(false);
        subjectObjClassBuilder.addAttributeInfo(memberOf.build());

        String[] extendedAttrs = configuration.getExtendedSubjectProperties();

        if (extendedAttrs != null) {

            List<String> extensionAttrs = Arrays.asList(extendedAttrs);
            for (String attr : extensionAttrs) {

                AttributeInfoBuilder extAttr = new AttributeInfoBuilder(attr);
                extAttr.setRequired(false).setType(String.class).setMultiValued(false)
                        .setCreateable(false).setUpdateable(false).setReadable(true)

                        .setReturnedByDefault(false);

                subjectObjClassBuilder.addAttributeInfo(extAttr.build());
            }

        }
        schemaBuilder.defineObjectClass(subjectObjClassBuilder.build());

    }

    public void executeQuery(Filter filter, ResultsHandler handler, OperationOptions operationOptions
            , Connection connection) {
        LOG.ok("Processing trough executeQuery methods for the object class {0}",
                SUBJECT_NAME);
        QueryBuilder queryBuilder = null;

        List<String> extended = configuration.getExtendedSubjectProperties() != null ?
                Arrays.asList(configuration.getExtendedSubjectProperties()) : null;

        if (getAttributesToGet(operationOptions) != null &&
                (!getAttributesToGet(operationOptions).isEmpty() && filter != null)) {

            Map<String, Map<String, Class>> tablesAndColumns = new HashMap<>();
            Map<Map<String, String>, String> joinMap = new HashMap<>();

            tablesAndColumns.put(TABLE_SU_NAME, columns);

            if (getAttributesToGet(operationOptions).contains(ATTR_MEMBER_OF)) {

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, membershipColumns);
                joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
            }

            if (getAttributesToGet(operationOptions).stream().anyMatch(atg -> extended.contains(atg))) {

                tablesAndColumns.put(TABLE_SU_EXTENSION_NAME, extensionColumns);
                joinMap.put(Map.of(TABLE_SU_EXTENSION_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
            }


            queryBuilder = new QueryBuilder(new ObjectClass(SUBJECT_NAME), filter,
                    tablesAndColumns, TABLE_SU_NAME, joinMap, operationOptions);
        } else {

            queryBuilder = new QueryBuilder(new ObjectClass(SUBJECT_NAME), filter, Map.of(TABLE_SU_NAME, columns),
                    TABLE_SU_NAME, operationOptions);
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

                {

                    Map<String, Class> mergedColumns = new HashMap<>();
                    mergedColumns.putAll(columns);
                    mergedColumns.putAll(suMembershipColumns);
                    mergedColumns.putAll(extensionColumns);

                    GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_NAME, result, objectConstructionSchema,
                            multiValuedAttributesCatalogue, Map.of(ATTR_MEMBER_OF_NATIVE, ATTR_MEMBER_OF));
                    go.setObjectClass(O_CLASS);

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
            }

            if (objects.isEmpty()) {
                LOG.ok("Empty object set in execute query");
            } else {
                for (String objectName : objects.keySet()) {

                    LOG.info("The object name: {0}", objectName);

                    LOG.info("The object: {0}", objects.get(objectName).toString());

                    GrouperObject go = objects.get(objectName);
                    if (configuration.getExcludeDeletedObjects()) {
                        if (go.isDeleted()) {
                            LOG.ok("Following object omitted from evaluation, because it's deleted, identifier: "
                                    + go.getIdentifier());

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

            String errMessage = "Exception occurred during the Execute query operation while processing the query: "
                    + query + ". The object class being handled: " + O_CLASS + ". And evaluating the filter: " + filter;

            throw new ExceptionHandler().evaluateAndHandleException(e, true, false, errMessage);

        }
    }

    @Override
    protected String getMemberShipAttributeName() {
        return ATTR_MEMBER_OF;
    }

    @Override
    protected String getExtensionAttributeTableName() {
        return TABLE_SU_EXTENSION_NAME;
    }

    @Override
    protected String getMembershipTableName() {
        return TABLE_MEMBERSHIP_NAME;
    }

    @Override
    protected String getMainTableName() {
        return TABLE_SU_NAME;
    }

    @Override
    public void sync(SyncToken syncToken, SyncResultsHandler syncResultsHandler, OperationOptions operationOptions,
                     Connection connection) {
        Map<String, GrouperObject> objectMap = sync(syncToken, operationOptions, connection);

        SyncDeltaBuilder builder = new SyncDeltaBuilder();
        builder.setObjectClass(O_CLASS);

        for (String objID : objectMap.keySet()) {
            GrouperObject grouperObject = objectMap.get(objID);

            if (!sync(syncResultsHandler, O_CLASS, grouperObject)) {

                break;
            }
        }
    }

    @Override
    public LinkedHashMap<String, GrouperObject> sync(SyncToken syncToken, OperationOptions operationOptions,
                                                     Connection connection) {
        QueryBuilder queryBuilder;
        LinkedHashMap<String, GrouperObject> objects = new LinkedHashMap<>();
        String tokenVal;
        if (syncToken.getValue() instanceof Long) {

            tokenVal = Long.toString((Long) syncToken.getValue());
        } else {
            tokenVal = (String) syncToken.getValue();
        }


        LOG.ok("The sync token value in the evaluation of subject processing sync method: {0}", tokenVal);

        GreaterThanFilter greaterThanFilterBase = (GreaterThanFilter)
                FilterBuilder.greaterThan(AttributeBuilder.build(TABLE_SU_NAME + "." + ATTR_MODIFIED,
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

            tablesAndColumns.put(TABLE_SU_NAME, Map.of(ATTR_DELETED, String.class,
                    ATTR_ID_IDX, Long.class, ATTR_MODIFIED, Long.class));

            Set<String> attrsToGet = getAttributesToGet(operationOptions);


            if (attrsToGet.contains(ATTR_MEMBER_OF)) {

                greaterThanFilterMember = (GreaterThanFilter)
                        FilterBuilder.greaterThan(AttributeBuilder.build(TABLE_MEMBERSHIP_NAME + "." + ATTR_MODIFIED,
                                tokenVal));

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, objectColumns);

                joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
            }

            if (attrsToGet.stream().anyMatch(atg -> extended.contains(atg))) {

                greaterThanFilterExtension = (GreaterThanFilter)
                        FilterBuilder.greaterThan(AttributeBuilder.build(TABLE_SU_EXTENSION_NAME + "." + ATTR_MODIFIED,
                                tokenVal));

                tablesAndColumns.put(TABLE_SU_EXTENSION_NAME, objectColumns);
                joinMap.put(Map.of(TABLE_SU_EXTENSION_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
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

            queryBuilder = new QueryBuilder(new ObjectClass(SUBJECT_NAME), filter,
                    tablesAndColumns, TABLE_SU_NAME, joinMap, operationOptions);
        } else {

            queryBuilder = new QueryBuilder(new ObjectClass(SUBJECT_NAME), filter, Map.of(TABLE_SU_NAME, columns),
                    TABLE_SU_NAME, operationOptions);
        }
        queryBuilder.setUseFullAlias(true);
        queryBuilder.setOrderByASC(CollectionUtil.newSet(ATTR_MODIFIED_LATEST));
        queryBuilder.setAsSyncQuery(true);

        String query = queryBuilder.build();

        ResultSet result = null;


        try {
            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();

            while (result.next()) {


                GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_NAME, result, objectConstructionSchema,
                        multiValuedAttributesCatalogue, null);
                go.setObjectClass(O_CLASS);

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
                LOG.ok("Empty object set in sync op.");
            } else {

                Map<String, GrouperObject> notDeletedObjects = new LinkedHashMap<>();

                for (String id : objects.keySet()) {
                    GrouperObject object = objects.get(id);

                    if (object.isDeleted()) {

                    } else {

                        notDeletedObjects.put(id, object);
                    }
                }

                if (!notDeletedObjects.isEmpty()) {
                    notDeletedObjects = fetchFullNonDeletedObjects(notDeletedObjects, operationOptions, connection);
                }

                for (String id : objects.keySet()) {

                    if (!notDeletedObjects.isEmpty() && notDeletedObjects.containsKey(id)) {

                        GrouperObject grouperObject = objects.get(id);
                        GrouperObject notDeletedObject = notDeletedObjects.get(id);

                        grouperObject.setName(notDeletedObject.getName());

                        Map<String, Object> attrMap = notDeletedObject.getAttributes();

                        for (String attName : attrMap.keySet()) {

                            grouperObject.addAttribute(attName, attrMap.get(attName), multiValuedAttributesCatalogue);

                        }

                    }

                }

            }

        } catch (SQLException e) {

            String errMessage = "Exception occurred during the Sync (liveSync) operation. " +
                    "The object class being handled: " + O_CLASS + ". While evaluating the token: " + tokenVal;

            throw new ExceptionHandler().evaluateAndHandleException(e, true, false, errMessage);

        }

        return objects;
    }

    @Override
    public Long getLatestSyncToken(Connection connection) {
        LOG.ok("Processing through the 'getLatestSyncToken' method for the objectClass {0}", ObjectClass.GROUP);

        Map<String, Map<String, Class>> tablesAndColumns = new HashMap<>();
        Map<Map<String, String>, String> joinMap = new HashMap<>();

        // Joining all tables related to object type
        tablesAndColumns.put(TABLE_SU_NAME, Map.of(ATTR_MODIFIED, Long.class));
        tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, Map.of(ATTR_MODIFIED, Long.class));
        tablesAndColumns.put(TABLE_SU_EXTENSION_NAME, Map.of(ATTR_MODIFIED, Long.class));

        joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
        joinMap.put(Map.of(TABLE_SU_EXTENSION_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);


        QueryBuilder queryBuilder = new QueryBuilder(O_CLASS, null,
                tablesAndColumns, TABLE_SU_NAME, joinMap, null);
        queryBuilder.setOrderByASC(CollectionUtil.newSet(ATTR_MODIFIED_LATEST));
        String query = queryBuilder.buildSyncTokenQuery();


        ResultSet result;
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

                    return val;
                }
            }

        } catch (SQLException e) {

            String errMessage = "Exception occurred during the Get Latest Sync Token operation." +
                    "The object class being handled: " + O_CLASS;

            throw new ExceptionHandler().evaluateAndHandleException(e, true, false, errMessage);

        }

        throw new ConnectorException("Latest sync token could not be fetched.");
    }

    private Map<String, GrouperObject> fetchFullNonDeletedObjects(Map<String, GrouperObject> notDeletedObject,
                                                                  OperationOptions operationOptions, Connection connection) {

        QueryBuilder queryBuilder;

        Set<String> idSet = new LinkedHashSet<>();
        for (String identifier : notDeletedObject.keySet()) {

            idSet.add(identifier);
        }

        List<String> extended = configuration.getExtendedSubjectProperties() != null ?
                Arrays.asList(configuration.getExtendedSubjectProperties()) : null;

        if (getAttributesToGet(operationOptions) != null &&
                !getAttributesToGet(operationOptions).isEmpty()) {


            Map<String, Map<String, Class>> tablesAndColumns = new HashMap<>();
            Map<Map<String, String>, String> joinMap = new HashMap<>();

            tablesAndColumns.put(TABLE_SU_NAME, columns);

            Set<String> attrsToGet = getAttributesToGet(operationOptions);


            if (attrsToGet.contains(ATTR_MEMBER_OF)) {

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, membershipColumns);
                joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
            }

            if (attrsToGet.stream().anyMatch(atg -> extended.contains(atg))) {

                tablesAndColumns.put(TABLE_SU_EXTENSION_NAME, extensionColumns);
                joinMap.put(Map.of(TABLE_SU_EXTENSION_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
            }

            queryBuilder = new QueryBuilder(new ObjectClass(SUBJECT_NAME), null,
                    tablesAndColumns, TABLE_SU_NAME, joinMap, operationOptions);
        } else {

            queryBuilder = new QueryBuilder(new ObjectClass(SUBJECT_NAME), null, Map.of(TABLE_SU_NAME, columns),
                    TABLE_SU_NAME, operationOptions);
        }

        queryBuilder.setUseFullAlias(true);
        queryBuilder.setInStatement(Map.of(TABLE_SU_NAME + "." + ATTR_UID, idSet));

        String query = queryBuilder.build();

        ResultSet result;

        Map<String, GrouperObject> objects = new HashMap<>();

        try {

            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();

            while (result.next()) {

                GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_NAME, result, objectConstructionSchema,
                        multiValuedAttributesCatalogue, Map.of(ATTR_MEMBER_OF_NATIVE, ATTR_MEMBER_OF));
                go.setObjectClass(O_CLASS);

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
                LOG.ok("Empty 'CREATE_OR_UPDATE' object set returned");
            }

            return objects;
        } catch (SQLException e) {

            String errMessage = "Exception occurred during the Sync (liveSync) operation. " +
                    "The object class being handled: " + O_CLASS + ". Evaluation interrupted while processing objects" +
                    "from the CREATE_OR_UPDATE set.";

            throw new ExceptionHandler().evaluateAndHandleException(e, true, false, errMessage);

        }
    }


    public Set<String> fetchExtensionSchema(Connection connection) throws SQLException {

        ResultSet result;
        QueryBuilder queryBuilder = new QueryBuilder(O_CLASS, TABLE_SU_EXTENSION_NAME, 1000);
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
