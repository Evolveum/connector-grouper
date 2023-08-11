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
import org.identityconnectors.framework.spi.SearchResultsHandler;

import java.sql.*;
import java.util.*;

public class SubjectProcessing extends ObjectProcessing {

    private static final Log LOG = Log.getLog(SubjectProcessing.class);
    private static final String ATTR_ID = "subject_id";
    private static final String ATTR_ID_IDX = "subject_id_index";
    protected static final String TABLE_SU_NAME = "gr_mp_subjects";
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

        ObjectClassInfoBuilder subjectObjClassBuilder = new ObjectClassInfoBuilder();

        subjectObjClassBuilder.setType("subject");


        //Read-only,
        AttributeInfoBuilder id = new AttributeInfoBuilder(Name.NAME);
        id.setRequired(true).setType(String.class).setCreateable(false).setUpdateable(false).setReadable(true)
                .setNativeName(ATTR_ID);
        subjectObjClassBuilder.addAttributeInfo(id.build());


        AttributeInfoBuilder last_modified = new AttributeInfoBuilder(ATTR_MODIFIED);
        last_modified.setRequired(false).setType(Integer.class).setCreateable(false).setUpdateable(false)
                .setReadable(true);
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
        LOG.ok("Processing through executeQuery methods for the object class {0}",
                SUBJECT_NAME);
        QueryBuilder queryBuilder;
        Boolean isEqualsUid = false;
        Boolean isAllQuery = !(filter != null);
        Boolean isPagedSearch = false;
        Integer maxPageSize = configuration.getMaxPageSize();
        Integer pageSize = null;

        if (filter != null && filter instanceof EqualsFilter) {

            if (((EqualsFilter) filter).getAttribute().getName().equals(Uid.NAME)) {
                isEqualsUid = true;
            }
        }

        List<String> extended = configuration.getExtendedSubjectProperties() != null ?
                Arrays.asList(configuration.getExtendedSubjectProperties()) : null;

        if (operationOptions != null && operationOptions.getPageSize() != null) {

            isPagedSearch = configuration.getEnableIdBasedPaging();
        }

        if (configuration.getExcludeDeletedObjects()) {
            if (!isAllQuery) {
                LOG.ok("Augmenting filter {0}, " +
                        "with DELETED=F argument based on the exclude delete objects property value", filter);

                EqualsFilter equalsFilter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build(
                        TABLE_SU_NAME + "." + ATTR_DELETED, "F"));

                filter = FilterBuilder.and(equalsFilter, filter);
            } else {

                LOG.ok("Augmenting empty filter with DELETED=F argument based on the exclude delete objects property " +
                        "value");

                filter = FilterBuilder.equalTo(AttributeBuilder.build(
                        TABLE_SU_NAME + "." + ATTR_DELETED, "F"));
            }
        }

        if (getAttributesToGet(operationOptions) != null &&
                (!getAttributesToGet(operationOptions).isEmpty() && !isAllQuery
                        && !isPagedSearch)) {

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
        Integer count = null;
        if (maxPageSize != null && !isEqualsUid) {

            if (pageSize != null) {

                if (pageSize > maxPageSize) {

                    count = countAll(queryBuilder.clone(), connection);
                    queryBuilder.setTotalCount(count);
                }
            } else {

                count = countAll(queryBuilder.clone(), connection);
                queryBuilder.setTotalCount(count);
            }
        }

        if (count == null) {

            handleExecuteQuery(handler, connection, queryBuilder, isAllQuery, isPagedSearch, operationOptions);

        } else {
            if (count < maxPageSize) {

                handleExecuteQuery(handler, connection, queryBuilder, isAllQuery, isPagedSearch, operationOptions);
            } else {
                for (int i = 0; count >= i; i = i + maxPageSize) {

                    queryBuilder.setPageSize(maxPageSize);
                    queryBuilder.setPageOffset(i + 1);

                    handleExecuteQuery(handler, connection, queryBuilder, isAllQuery, isPagedSearch, operationOptions);
                }
            }
        }
    }

    protected void handleExecuteQuery(ResultsHandler handler, Connection connection, QueryBuilder queryBuilder,
                                      Boolean isAllQuery, Boolean isPagedSearch, OperationOptions operationOptions) {
        ResultSet result;
        String query = queryBuilder.build();

        LOG.info("Query about to be executed: {0}", query);
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

            String pseudoCookie = null;
            if (objects.isEmpty()) {
                LOG.ok("Empty object set in execute query");
            } else {

                if (!isAllQuery && isPagedSearch) {
                    objects = fetchFullObjects(objects, operationOptions, connection);
                }

                Integer sizeS = objects.size();
                Integer processed = 0;
                for (String objectName : objects.keySet()) {

                    LOG.info("The object name: {0}", objectName);

                    LOG.info("The object: {0}", objects.get(objectName).toString());

                    GrouperObject go = objects.get(objectName);

                    ConnectorObjectBuilder co = buildConnectorObject(O_CLASS, go, operationOptions);

                    pseudoCookie = go.getIdentifier();
                    if (!handler.handle(co.build())) {

                        if (handler instanceof SearchResultsHandler) {

                            LOG.ok("Handling results with pseudoCookie: {0}", pseudoCookie);
                            LOG.ok("Remaining page results: {0}", sizeS - processed);

                            SearchResult searchResult = new SearchResult(pseudoCookie,
                                    sizeS - processed);
                            ((SearchResultsHandler) handler).handleResult(searchResult);
                        }

                        LOG.warn("Result handling interrupted by handler!");
                        break;
                    }

                    processed++;
                }

                if (handler instanceof SearchResultsHandler) {

                    LOG.ok("Handling results with pseudoCookie: {0}", pseudoCookie);
                    LOG.ok("Remaining page results: {0}", sizeS - processed);

                    SearchResult searchResult = new SearchResult(pseudoCookie, -1);
                    ((SearchResultsHandler) handler).handleResult(searchResult);
                }
            }

        } catch (SQLException e) {

            String errMessage = "Exception occurred during the Execute query operation while processing the query: "
                    + query + ". The object class being handled: " + O_CLASS + ".";

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

        QueryBuilder syncQueryBuilder = syncQuery(syncToken, operationOptions, connection);
        Integer totalCount = syncQueryBuilder.getTotalCount();

        SyncDeltaBuilder builder = new SyncDeltaBuilder();
        builder.setObjectClass(O_CLASS);

        if (totalCount != null) {

            Integer maxPageSize = configuration.getMaxPageSize();
            Integer pageSize = null;

            if (operationOptions.getOptions().containsKey(OperationOptions.OP_PAGE_SIZE)) {

                pageSize = operationOptions.getPageSize();
            }

            if (pageSize != null) {

                if (pageSize > maxPageSize) {

                    handleLargerThanMaxSize(O_CLASS, syncResultsHandler, syncToken, syncQueryBuilder,
                            operationOptions, connection, totalCount, maxPageSize);
                }
            } else {

                if (totalCount > maxPageSize) {

                    handleLargerThanMaxSize(O_CLASS, syncResultsHandler, syncToken, syncQueryBuilder,
                            operationOptions, connection, totalCount, maxPageSize);
                }
            }

        } else {

            Map<String, GrouperObject> objectMap = sync(syncToken, operationOptions, connection, syncQueryBuilder);

            for (String objID : objectMap.keySet()) {
                GrouperObject grouperObject = objectMap.get(objID);

                if (!sync(syncResultsHandler, O_CLASS, grouperObject)) {

                    break;
                }
            }
        }
    }

    @Override
    public LinkedHashMap<String, GrouperObject> sync(SyncToken syncToken, OperationOptions operationOptions,
                                                     Connection connection, QueryBuilder queryBuilder) {

        LinkedHashMap<String, GrouperObject> objects = new LinkedHashMap<>();

        String tokenVal;
        if (syncToken.getValue() instanceof Long) {

            tokenVal = Long.toString((Long) syncToken.getValue());
        } else {
            tokenVal = (String) syncToken.getValue();
        }

        ResultSet result = null;

        try {
            PreparedStatement prepareStatement = connection.prepareStatement(queryBuilder.build());
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
                    notDeletedObjects = fetchFullObjects(notDeletedObjects, operationOptions, connection);
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
        LOG.ok("Processing through the 'getLatestSyncToken' method for the objectClass {0}", GroupProcessing.O_CLASS);

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

    private Map<String, GrouperObject> fetchFullObjects(Map<String, GrouperObject> notDeletedObject,
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
                    tablesAndColumns, TABLE_SU_NAME, joinMap, null);
        } else {

            queryBuilder = new QueryBuilder(new ObjectClass(SUBJECT_NAME), null, Map.of(TABLE_SU_NAME, columns),
                    TABLE_SU_NAME, null);
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

    public QueryBuilder syncQuery(SyncToken syncToken, OperationOptions operationOptions, Connection connection) {
        QueryBuilder queryBuilder;

        String tokenVal;

        Integer maxPageSize = configuration.getMaxPageSize();
        Integer pageSize = null;

        if (operationOptions.getOptions().containsKey(OperationOptions.OP_PAGE_SIZE)) {

            pageSize = operationOptions.getPageSize();
        }

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

                tablesAndColumns.put(TABLE_MEMBERSHIP_NAME, Map.of(ATTR_MODIFIED, Long.class));

                joinMap.put(Map.of(TABLE_MEMBERSHIP_NAME, ATTR_SCT_ID_IDX), ATTR_ID_IDX);
            }

            if (attrsToGet.stream().anyMatch(atg -> extended.contains(atg))) {

                greaterThanFilterExtension = (GreaterThanFilter)
                        FilterBuilder.greaterThan(AttributeBuilder.build(TABLE_SU_EXTENSION_NAME + "." + ATTR_MODIFIED,
                                tokenVal));

                tablesAndColumns.put(TABLE_SU_EXTENSION_NAME, Map.of(ATTR_MODIFIED, Long.class));
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


        if (maxPageSize != null) {

            if (pageSize != null) {

                if (pageSize > maxPageSize) {

                    Integer count = countAll(queryBuilder.clone(), connection);
                    queryBuilder.setTotalCount(count);
                }
            } else {

                Integer count = countAll(queryBuilder.clone(), connection);
                queryBuilder.setTotalCount(count);
            }
        }

        return queryBuilder;

    }
}
