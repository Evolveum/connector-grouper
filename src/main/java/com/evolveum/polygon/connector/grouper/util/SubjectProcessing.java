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
            //TODO test
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

        multiValuedAttributesCatalogue.add(ATTR_MEMBER_OF_NATIVE);
    }

    @Override
    public void buildObjectClass(SchemaBuilder schemaBuilder, GrouperConfiguration configuration) {
        LOG.info("Building object class definition for {0}", SUBJECT_NAME);

        ObjectClassInfoBuilder subjectObjClassBuilder = new ObjectClassInfoBuilder();
        subjectObjClassBuilder.setType(SUBJECT_NAME);


        // TODO comment up
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
                        //TODO should this be returned by default
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

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, suMembershipColumns);
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
        String query = queryBuilder.build();
        ResultSet result = null;

        LOG.info("Query about to be executed: {0}", query);

        try {

            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();
            //ConnectorObjectBuilder co = null;

            while (result.next()) {

                if (filter instanceof EqualsFilter) {

                    LOG.ok("Processing Equals Query");
                    final EqualsFilter equalsFilter = (EqualsFilter) filter;
                    Attribute fAttr = equalsFilter.getAttribute();
                    if (Uid.NAME.equals(fAttr.getName())) {

                        GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_ID, result,
                                columns, multiValuedAttributesCatalogue, null);

                        if (getAttributesToGet(operationOptions) != null &&
                                !getAttributesToGet(operationOptions).isEmpty()) {

                            populateOptionalAttributes(result, go, configuration);
                        }

                        ConnectorObjectBuilder co = buildConnectorObject(O_CLASS, go, operationOptions);

                        if (configuration.getExcludeDeletedObjects()) {
                            if (go.isDeleted()) {
                                LOG.ok("Following object omitted from evaluation, because it's deleted" +
                                        "identifier: {0} ; name: {1}.", go.getIdentifier(), go.getName());

                                continue;
                            }
                        }

                        handler.handle(co.build());
                        break;

                    } else {

                        GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_ID, result,
                                columns, multiValuedAttributesCatalogue, null);

                        ConnectorObjectBuilder co = buildConnectorObject(O_CLASS, go, operationOptions);

                        if (configuration.getExcludeDeletedObjects()) {
                            if (go.isDeleted()) {
                                LOG.ok("Following object omitted from evaluation, because it's deleted" +
                                        "identifier: {0} ; name: {1}.", go.getIdentifier(), go.getName());

                                continue;
                            }
                        }

                        handler.handle(co.build());
                    }
                } else {

                    GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_ID, result,
                            columns, multiValuedAttributesCatalogue, null);

                    ConnectorObjectBuilder co = buildConnectorObject(O_CLASS, go, operationOptions);


                    if (configuration.getExcludeDeletedObjects()) {
                        if (go.isDeleted()) {
                            LOG.ok("Following object omitted from evaluation, because it's deleted" +
                                    "identifier: {0} ; name: {1}.", go.getIdentifier(), go.getName());

                            continue;
                        }
                    }

                    handler.handle(co.build());
                }
            }

        } catch (SQLException e) {

            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getMemberShipAttributeName() {
        return ATTR_MEMBER_OF_NATIVE;
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
    protected GrouperObject populateOptionalAttributes(ResultSet result,
                                                       GrouperObject ob,
                                                       GrouperConfiguration configuration)
            throws SQLException {

        LOG.info("Evaluating membership attribute values.");

        HashMap<String, Set<Object>> multiValues = new HashMap<>();

        buildOptional(result, Collections.singletonMap(ATTR_GR_ID_IDX, Long.class), multiValues, configuration);


        //TODO commented for sync test
        while (result.next()) {

            buildOptional(result, Collections.singletonMap(ATTR_GR_ID_IDX, Long.class), multiValues, configuration);
        }

        for (String attrName : multiValues.keySet()) {
            LOG.ok("Adding attribute values for the attribute {0} to the attribute builder.", attrName);

            if (ATTR_GR_ID_IDX.equals(attrName)) {

                ob.addAttribute(ATTR_MEMBER_OF, multiValues.get(attrName), multiValuedAttributesCatalogue);
            } else {

                ob.addAttribute(attrName, multiValues.get(attrName), multiValuedAttributesCatalogue);
            }
        }

        return ob;

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
//            tablesAndColumns.put(TABLE_SU_NAME, columns);

            Set<String> attrsToGet = getAttributesToGet(operationOptions);


            if (attrsToGet.contains(ATTR_MEMBER_OF)) {

                greaterThanFilterMember = (GreaterThanFilter)
                        FilterBuilder.greaterThan(AttributeBuilder.build(TABLE_MEMBERSHIP_NAME + "." + ATTR_MODIFIED,
                                tokenVal));

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, objectColumns);
                // tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, suMembershipColumns);
                joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
            }

            if (attrsToGet.stream().anyMatch(atg -> extended.contains(atg))) {

                greaterThanFilterExtension = (GreaterThanFilter)
                        FilterBuilder.greaterThan(AttributeBuilder.build(TABLE_SU_EXTENSION_NAME + "." + ATTR_MODIFIED,
                                tokenVal));

                tablesAndColumns.put(TABLE_SU_EXTENSION_NAME, objectColumns);
//                tablesAndColumns.put(TABLE_SU_EXTENSION_NAME, extensionColumns);
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

        Map<String, GrouperObject> objects = new LinkedHashMap<>();
        try {
            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();

            while (result.next()) {

                //TODO
//                Map<String, Class> mergedColumns = new HashMap<>();
//                mergedColumns.putAll(columns);
//                mergedColumns.putAll(suMembershipColumns);
//                mergedColumns.putAll(extensionColumns);
                //TODO
//                columns.putAll(suMembershipColumns);
//                columns.putAll(extensionColumns);


                // TODO change object processing to incorporate multi valued processing (e.g. as in populate and
                //  build optional)
                GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_NAME, result, objectConstructionSchema,
                        multiValuedAttributesCatalogue, null);
                // TODO
                LOG.ok("Evaluated obj #:{0}", go.toString());
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
                //TODO
                Map<String, GrouperObject> notDeletedObject = new LinkedHashMap<>();

                for (String id : objects.keySet()) {
                    GrouperObject object = objects.get(id);

                    if (object.isDeleted()) {

//                        builder.setDeltaType(SyncDeltaType.DELETE);
//                        LOG.ok("### {0} is deleted", id);
//                        builder.setUid(new Uid(id));
//                        builder.setToken(new SyncToken(object.getLatestTimestamp()));
//
//                        syncResultsHandler.handle(builder.build());
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
                    // TODO remove
                    LOG.ok("The returned sync delta: {0}, The OID {1}", syncdelta, syncdelta.getUid() == null ?
                            "null" : syncdelta.getUid());

                    if (!syncResultsHandler.handle(syncdelta)) {

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
        tablesAndColumns.put(TABLE_SU_NAME, Map.of(ATTR_MODIFIED, Long.class));
        tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, Map.of(ATTR_MODIFIED, Long.class));
        tablesAndColumns.put(TABLE_SU_EXTENSION_NAME, Map.of(ATTR_MODIFIED, Long.class));

        joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
        joinMap.put(Map.of(TABLE_SU_EXTENSION_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);


        QueryBuilder queryBuilder = new QueryBuilder(O_CLASS, null,
                tablesAndColumns, TABLE_SU_NAME, joinMap, null);
        queryBuilder.setOrderByASC(CollectionUtil.newSet(ATTR_MODIFIED_LATEST));
        // LOG for now TODO remove
        String query = queryBuilder.buildSyncTokenQuery();
        LOG.ok("The latest sync token query: {0}", query);

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
                                                                  OperationOptions operationOptions, Connection connection) {

        QueryBuilder queryBuilder;
        //TODO clean up
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
// TODO remove log
            columns.keySet().forEach(key -> LOG.ok("### The column name {0}", key));
            Set<String> attrsToGet = getAttributesToGet(operationOptions);


            if (attrsToGet.contains(ATTR_MEMBER_OF)) {

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, suMembershipColumns);
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

        ResultSet result = null;

        Map<String, GrouperObject> objects = new HashMap<>();

        try {

            PreparedStatement prepareStatement = connection.prepareStatement(query);
            result = prepareStatement.executeQuery();

            while (result.next()) {

//                Map<String, Class> mergedColumns = new HashMap<>();
//                mergedColumns.putAll(columns);
//                mergedColumns.putAll(suMembershipColumns);
//                mergedColumns.putAll(extensionColumns);


                // TODO change object processing to incorporate multi valued processing (e.g. as in populate and
                //  build optional)

                GrouperObject go = buildGrouperObject(ATTR_UID, ATTR_NAME, result, objectConstructionSchema,
                        multiValuedAttributesCatalogue, Map.of(ATTR_MEMBER_OF_NATIVE, ATTR_MEMBER_OF));
                // TODO
                LOG.ok("Evaluated obj #:{0}", go.toString());
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


    private void buildOptional(ResultSet result, Map<String, Class> columns,
                               HashMap<String, Set<Object>> multiValues, GrouperConfiguration configuration)
            throws SQLException {

        LOG.info("Evaluation of SQL objects present in result set for multivalued attributes.");

        ResultSetMetaData meta = result.getMetaData();

        String extAttrName = null;
        String etxAttrValue = null;

        String[] subjectImProperties = configuration.getExtendedSubjectProperties();

        List<String> extSubjectProperties = null;


        if (subjectImProperties != null && subjectImProperties.length > 0) {

            extSubjectProperties = List.of(subjectImProperties);
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

                    LOG.ok("Addition of Long type attribute for attribute from column with name " +
                            "{0} to the multivalued collection", name);

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

        if (extSubjectProperties != null) {

            if (extAttrName != null && extSubjectProperties.contains(extAttrName)) {

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
