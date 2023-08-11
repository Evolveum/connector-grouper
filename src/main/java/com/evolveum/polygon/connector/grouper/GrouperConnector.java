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

package com.evolveum.polygon.connector.grouper;

import com.evolveum.polygon.connector.grouper.util.*;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import java.sql.SQLException;
import java.util.*;

/**
 * midPoint connector for InCommon Grouper
 * <p>
 * The connector acts as a read only connector. Capabilities include also live synchronization using
 * a 'modified' time stamp column and 'deleted' colum for updates.
 * <p>
 * Connector reads data trough multiple database tables related to both supported object types. The rows related to
 * a specific objects are then merged and from this a specific object is constructed and handled.
 * <p>
 *
 * @author Matus Macik
 * @since 1.0
 */
@ConnectorClass(displayNameKey = "grouper.connector.display", configurationClass = GrouperConfiguration.class)
public class GrouperConnector implements Connector, SchemaOp, TestOp, SearchOp<Filter>, DiscoverConfigurationOp,
        SyncOp {

    /**
     * Instance of Log, for logging of the class {@link GrouperConnector}.
     */
    private static final Log LOG = Log.getLog(GrouperConnector.class);

    /**
     * Instance of {@link Configuration}. Initialized via callback at
     * {@link GrouperConnector#init(Configuration)}
     */
    private GrouperConfiguration configuration;

    /**
     * Instance of {@link GrouperConnection}. This class handles the connection to the grouper repository.
     * The class created an instance of {@link java.sql.Connection} which is consumed by the underlying connector
     * methods.
     */
    private GrouperConnection grouperConnection;

    /**
     * Accessor for {@link Configuration}.
     */
    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Initialization of basic connector parameters.
     */
    @Override
    public void init(Configuration configuration) {

        this.configuration = (GrouperConfiguration) configuration;
        this.grouperConnection = new GrouperConnection(this.configuration);

    }

    /**
     * Cleanup method for connector.
     */
    @Override
    public void dispose() {
        configuration = null;
        if (grouperConnection != null) {
            grouperConnection.dispose();
            grouperConnection = null;
        }
    }

    /**
     * Generated the schema for both object classes supported by the connector. The schema is partially hardcoded
     * and partially might be dynamically computed based on extension attributes. Method uses a utility class instance
     * of {@link SchemaTranslator} which handles the specifics.
     */
    @Override
    public Schema schema() {
        LOG.info("Evaluating the schema operation");
        SchemaTranslator translator = new SchemaTranslator();

        return translator.generateSchema(configuration);

    }


    @Override
    public FilterTranslator createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {

        return new FilterTranslator<Filter>() {
            @Override
            public List<Filter> translate(Filter filter) {
                return CollectionUtil.newList(filter);
            }
        };
    }

    /**
     * Method used to search the data source for object data. Method uses {@link Filter} type objects which are
     * translated and execute as a native SQL query. Based on the {@link ObjectClass}, the specific tables and
     * table data related to the concrete object class is selected. Based on the {@link OperationOptions}, the method
     * might fetch auxiliary attributes and use pagination, based on this it will modify the native query and output data.
     */
    @Override
    public void executeQuery(ObjectClass objectClass, Filter filter, ResultsHandler resultsHandler, OperationOptions
            operationOptions) {

        LOG.info("Processing through the executeQuery operation using the object class: {0}", objectClass);
        LOG.ok("The filter(s) used for the execute query operation:{0} ", filter == null ? "Empty filter, fetching" +
                " all objects of the object type." : filter);
        LOG.ok("Evaluating executeQuery with the following operation options: {0}", operationOptions == null ? "empty" +
                "operation options." : operationOptions);

        if (objectClass == null) {

            throw new IllegalArgumentException("Object class attribute can no be null");
        }

        if (resultsHandler == null) {
            LOG.error("Attribute of type ResultsHandler not provided.");
            throw new InvalidAttributeValueException("Attribute of type ResultsHandler is not provided.");
        }


        if (objectClass.is(ObjectProcessing.SUBJECT_NAME)) {
            SubjectProcessing subjectProcessing = new SubjectProcessing(configuration);

            LOG.ok("The object class for which the filter will be executed: {0}", objectClass.getDisplayNameKey());

            subjectProcessing.executeQuery(filter, resultsHandler, operationOptions, grouperConnection.getConnection());

        }


        if (objectClass.is(ObjectProcessing.GROUP_NAME)) {
            GroupProcessing groupProcessing = new GroupProcessing(configuration);

            LOG.ok("The object class for which the filter will be executed: {0}", objectClass.getDisplayNameKey());

            groupProcessing.executeQuery(filter, resultsHandler, operationOptions, grouperConnection.getConnection());

        }

        LOG.ok("Finished evaluating the execute query operation.");
    }

    /**
     * Basic test operation which uses a simple query to evaluate if the connector configuration is valid and
     * the connection is working.
     */
    @Override
    public void test() {
        LOG.info("Executing test operation.");
        configuration.validate();
        grouperConnection.test();
        grouperConnection.dispose();

        LOG.ok("Test OK");
    }

    @Override
    public void testPartialConfiguration() {
        // Test method would be equal to 'test()', so left empty so there is no additional overhead.

    }

    /**
     * Method generates a set of initial configuration values. These can be used in the interaction with the remote
     * system or can be overriden. Also the method produces a list of names of the auxiliary attributes present in the auxiliary
     * attribute tables. It is then possible to choose from this list which attributes should be present in the
     * resource schema.
     */
    @Override
    public Map<String, SuggestedValues> discoverConfiguration() {
        Map<String, SuggestedValues> suggestions = new HashMap<>();

        Integer connectionValidTimeout = configuration.getConnectionValidTimeout();
        Boolean excludeDeletedObjects = configuration.getExcludeDeletedObjects();
        Boolean enableIdBasedPaging = configuration.getEnableIdBasedPaging();

        if (connectionValidTimeout != null) {

            suggestions.put("connectionValidTimeout", SuggestedValuesBuilder.buildOpen(connectionValidTimeout));
        } else {

            // Default for connectionValidTimeout
            suggestions.put("timeout", SuggestedValuesBuilder.buildOpen("10"));
        }

        if (excludeDeletedObjects != null) {

            suggestions.put("excludeDeletedObjects", SuggestedValuesBuilder.buildOpen(excludeDeletedObjects));
        } else {

            suggestions.put("excludeDeletedObjects", SuggestedValuesBuilder.buildOpen(true));
        }

        if (enableIdBasedPaging != null) {

            suggestions.put("enableIdBasedPaging", SuggestedValuesBuilder.buildOpen(enableIdBasedPaging));
        } else {

            suggestions.put("enableIdBasedPaging", SuggestedValuesBuilder.buildOpen(false));
        }

        suggestions.put("extendedGroupProperties", SuggestedValuesBuilder.buildOpen(
                fetchExtensionAttributes(GroupProcessing.O_CLASS) != null ?
                        fetchExtensionAttributes(GroupProcessing.O_CLASS).toArray(new String[0]) : null
        ));

        suggestions.put("extendedSubjectProperties", SuggestedValuesBuilder.buildOpen(
                fetchExtensionAttributes(SubjectProcessing.O_CLASS) != null ?
                        fetchExtensionAttributes(SubjectProcessing.O_CLASS).toArray(new String[0]) : null
        ));

        return suggestions;
    }

    private Set<String> fetchExtensionAttributes(ObjectClass oClass) {
        LOG.info("Fetching extension attributes for the object class {0}", oClass);

        if (oClass.equals(GroupProcessing.O_CLASS)) {

            GroupProcessing processing = new GroupProcessing(configuration);

            try {
                return processing.fetchExtensionSchema(grouperConnection.getConnection());

            } catch (SQLException e) {

                String errMessage = "Exception occurred while fetching the extension attributes for dynamic schema " +
                        "evaluation. The object class being handled: " + oClass;

                throw new ExceptionHandler().evaluateAndHandleException(e, true, false, errMessage);
            }

        } else if (oClass.equals(SubjectProcessing.O_CLASS)) {

            SubjectProcessing processing = new SubjectProcessing(configuration);

            try {
                return processing.fetchExtensionSchema(grouperConnection.getConnection());

            } catch (SQLException e) {
                String errMessage = "Exception occurred while fetching the extension attributes for dynamic schema " +
                        "evaluation. The object class being handled: " + oClass;

                throw new ExceptionHandler().evaluateAndHandleException(e, true, false, errMessage);
            }

        } else {

            throw new ConnectorException("Unexpected object class used in extension attribute evaluation.");
        }

    }

    /**
     * Implementation of SyncOP sync method. Method requests changes which are used for synchronization of data.
     * Synchronization events are ordered based on a 'modified' time stamp column which values are
     * used as the {@link SyncToken} value. The syncToken is used also in each call, when a query is generated to fetch
     * rows marked with a time stamp value following the value of syncToken.
     * Based on the {@link ObjectClass}, the specific tables and table data related to the concrete object class is
     * selected. Based on the {@link OperationOptions}, the method might fetch auxiliary attributes and use pagination.
     * The "deleted" column is used to mark objects or rows related to an object as removed.
     */
    @Override
    public void sync(ObjectClass objectClass, SyncToken syncToken, SyncResultsHandler syncResultsHandler,
                     OperationOptions operationOptions) {

        LOG.ok("Evaluation of SYNC op method regarding the object class {0} with the following options: {1}", objectClass
                , operationOptions);

        if (syncToken == null) {

            LOG.ok("Empty token, fetching latest sync token");
            syncToken = getLatestSyncToken(objectClass);
        }


        if (objectClass.is(ObjectProcessing.GROUP_NAME)) {
            GroupProcessing groupProcessing = new GroupProcessing(configuration);
            groupProcessing.sync(syncToken, syncResultsHandler, operationOptions, grouperConnection.getConnection());

        } else if (objectClass.is(ObjectProcessing.SUBJECT_NAME)) {
            SubjectProcessing subjectProcessing = new SubjectProcessing(configuration);
            subjectProcessing.sync(syncToken, syncResultsHandler, operationOptions, grouperConnection.getConnection());

        } else if (objectClass.is(ObjectClass.ALL_NAME)) {

            SubjectProcessing subjectProcessing = new SubjectProcessing(configuration);
            GroupProcessing groupProcessing = new GroupProcessing(configuration);
            LinkedHashMap<String, GrouperObject> subjectObjectLinkedHashMap = new LinkedHashMap<>();
            LinkedHashMap<String, GrouperObject> groupObjectLinkedHashMap = new LinkedHashMap<>();
            Integer maxPageSize = configuration.getMaxPageSize();


            QueryBuilder subjectQuery = subjectProcessing.syncQuery(syncToken, operationOptions,
                    grouperConnection.getConnection());
            QueryBuilder groupQuery = groupProcessing.syncQuery(syncToken, operationOptions,
                    grouperConnection.getConnection());

            Integer subjectCount = subjectQuery.getTotalCount();
            Integer groupCount = groupQuery.getTotalCount();
            if (maxPageSize != null) {
                if (subjectCount != null) {

                    for (int i = 0; subjectCount >= i; i = i + maxPageSize) {

                        subjectQuery.setPageSize(maxPageSize);
                        subjectQuery.setPageOffset(i + 1);

                        subjectObjectLinkedHashMap.putAll(subjectProcessing.
                                sync(syncToken, operationOptions, grouperConnection.getConnection(), subjectQuery));

                    }
                }

                if (groupCount != null) {

                    for (int i = 0; groupCount >= i; i = i + maxPageSize) {

                        groupQuery.setPageSize(maxPageSize);
                        groupQuery.setPageOffset(i + 1);

                        groupObjectLinkedHashMap.putAll(groupProcessing.
                                sync(syncToken, operationOptions, grouperConnection.getConnection(), groupQuery));

                    }
                }

            } else {

                subjectObjectLinkedHashMap = subjectProcessing.
                        sync(syncToken, operationOptions, grouperConnection.getConnection(), subjectQuery);

                groupObjectLinkedHashMap = groupProcessing.
                        sync(syncToken, operationOptions, grouperConnection.getConnection(), groupQuery);
            }


            LinkedHashMap<String, GrouperObject> mergedMap = new LinkedHashMap<>();

            Iterator<String> groupIterator = groupObjectLinkedHashMap.keySet().iterator();
            Iterator<String> subjectIterator = subjectObjectLinkedHashMap.keySet().iterator();

            GrouperObject grouperGroup = null;
            Long groupTimestamp = null;

            while (subjectIterator.hasNext()) {
                GrouperObject so = subjectObjectLinkedHashMap.get(subjectIterator.next());
                Long subjectTimestamp = so.latestTimestamp;

                while (groupIterator.hasNext()) {

                    if (grouperGroup == null) {
                        grouperGroup = groupObjectLinkedHashMap.get(groupIterator.next());
                    }
                    if (groupTimestamp == null) {
                        groupTimestamp = grouperGroup.latestTimestamp;
                    }

                    if (groupTimestamp.compareTo(subjectTimestamp) <= 0) {
                        mergedMap.put(grouperGroup.getIdentifier(), grouperGroup);

                        if (!groupIterator.hasNext()) {
                            grouperGroup = null;

                        } else {
                            grouperGroup = groupObjectLinkedHashMap.get(groupIterator.next());
                            groupTimestamp = grouperGroup.latestTimestamp;

                        }
                    } else {
                        mergedMap.put(so.getIdentifier(), so);

                        break;
                    }
                }
                if (!groupIterator.hasNext()) {

                    if (grouperGroup != null) {

                        if (groupTimestamp.compareTo(subjectTimestamp) <= 0) {
                            mergedMap.put(grouperGroup.getIdentifier(), grouperGroup);
                            mergedMap.put(so.getIdentifier(), so);
                            grouperGroup = null;
                        } else {
                            mergedMap.put(so.getIdentifier(), so);
                        }
                    } else {
                        mergedMap.put(so.getIdentifier(), so);
                    }
                }
            }

            if (groupIterator.hasNext()) {

                while (groupIterator.hasNext()) {

                    if (grouperGroup != null) {
                        mergedMap.put(grouperGroup.getIdentifier(), grouperGroup);
                        grouperGroup = groupObjectLinkedHashMap.get(groupIterator.next());

                        if (!groupIterator.hasNext()) {
                            mergedMap.put(grouperGroup.getIdentifier(), grouperGroup);
                        }
                    } else {
                        grouperGroup = groupObjectLinkedHashMap.get(groupIterator.next());
                        mergedMap.put(grouperGroup.getIdentifier(), grouperGroup);
                    }

                }
            } else {

                if (grouperGroup != null) {

                    mergedMap.put(grouperGroup.getIdentifier(), grouperGroup);
                }
            }

            for (String id : mergedMap.keySet()) {

                GrouperObject go = mergedMap.get(id);

                    if (go.getObjectClass().is(ObjectProcessing.GROUP_NAME)) {

                    if (!groupProcessing.sync(syncResultsHandler, GroupProcessing.O_CLASS, go)) {

                        break;
                    }
                } else if (go.getObjectClass().is(SubjectProcessing.SUBJECT_NAME)) {

                    if (!subjectProcessing.sync(syncResultsHandler, SubjectProcessing.O_CLASS, go)) {

                        break;
                    }
                }
            }

        } else {

            throw new UnsupportedOperationException("Attribute of type" + objectClass + "is not supported. " +
                    "Only " + GroupProcessing.GROUP_NAME + " and " + ObjectProcessing.SUBJECT_NAME + " objectclass " +
                    "is supported for SyncOp currently.");
        }
    }

    /**
     * Method fetches the latest "modified" time stamp value of the specific object class, and based on it
     * creates and instance of {@link SyncToken}. In case of the "ALL" {@link ObjectClass}, the token will be the
     * later one from the Subject and Group object classes compared.
     */
    @Override
    public SyncToken getLatestSyncToken(ObjectClass objectClass) {


        if (objectClass.is(ObjectProcessing.GROUP_NAME)) {

            GroupProcessing groupProcessing = new GroupProcessing(configuration);

            return new SyncToken(groupProcessing.getLatestSyncToken(grouperConnection.getConnection()));

        } else if (objectClass.is(ObjectProcessing.SUBJECT_NAME)) {

            SubjectProcessing subjectProcessing = new SubjectProcessing(configuration);

            return new SyncToken(subjectProcessing.getLatestSyncToken(grouperConnection.getConnection()));
        } else if (objectClass.is(ObjectClass.ALL_NAME)) {

            GroupProcessing groupProcessing = new GroupProcessing(configuration);
            SubjectProcessing subjectProcessing = new SubjectProcessing(configuration);

            Long subjectToken = subjectProcessing.getLatestSyncToken(grouperConnection.getConnection());

            Long groupToken = groupProcessing.getLatestSyncToken(grouperConnection.getConnection());

            if (subjectToken.compareTo(groupToken) <= 0) {

                return new SyncToken(groupToken);
            } else {

                return new SyncToken(subjectToken);
            }

        } else {

            throw new UnsupportedOperationException("Attribute of type" + objectClass + "is not supported. " +
                    "Only " + GroupProcessing.GROUP_NAME + " and " + ObjectProcessing.SUBJECT_NAME + " objectclass " +
                    "is supported for SyncOp currently.");
        }
    }
}
