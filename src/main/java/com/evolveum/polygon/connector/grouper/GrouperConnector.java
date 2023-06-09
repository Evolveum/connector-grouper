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
import org.identityconnectors.framework.spi.operations.DiscoverConfigurationOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.TestOp;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ConnectorClass(displayNameKey = "grouper.connector.display", configurationClass = GrouperConfiguration.class)
public class GrouperConnector implements Connector, SchemaOp, TestOp, SearchOp<Filter>, DiscoverConfigurationOp {

    private static final Log LOG = Log.getLog(GrouperConnector.class);

    private GrouperConfiguration configuration;
    private GrouperConnection connection;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {

        this.configuration = (GrouperConfiguration) configuration;
        this.connection = new GrouperConnection(this.configuration);

    }

    @Override
    public void dispose() {
        configuration = null;
        if (connection != null) {
            connection.dispose();
            connection = null;
        }
    }

    @Override
    public Schema schema() {
        LOG.info("Evaluating the schema operation");
        SchemaTranslator translator = new SchemaTranslator();

        return translator.generateSchema(configuration);

    }

    @Override
    public FilterTranslator createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {

        // TODO using filter visitor pattern
        return new FilterTranslator<Filter>() {
            @Override
            public List<Filter> translate(Filter filter) {
                return CollectionUtil.newList(filter);
            }
        };
    }

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

            subjectProcessing.executeQuery(filter, resultsHandler, operationOptions, connection.getConnection());

        }

        if (objectClass.is(ObjectClass.GROUP_NAME)) {
            GroupProcessing groupProcessing = new GroupProcessing(configuration);

            LOG.ok("The object class for which the filter will be executed: {0}", objectClass.getDisplayNameKey());

            groupProcessing.executeQuery(filter, resultsHandler, operationOptions, connection.getConnection());

        }

        LOG.ok("Finished evaluating the execute query operation.");
    }

    @Override
    public void test() {
        LOG.info("Executing test operation.");
        connection.test();
        connection.dispose();

        LOG.ok("Test OK");
    }

    @Override
    public void testPartialConfiguration() {

    }

    @Override
    public Map<String, SuggestedValues> discoverConfiguration() {
        Map<String, SuggestedValues> suggestions = new HashMap<>();

        Integer connectionValidTimeout = configuration.getConnectionValidTimeout();

        if (connectionValidTimeout != null) {

            suggestions.put("connectionValidTimeout", SuggestedValuesBuilder.buildOpen(connectionValidTimeout));
        } else {

            // Default for connectionValidTimeout
            suggestions.put("timeout", SuggestedValuesBuilder.buildOpen("10"));
        }

        suggestions.put("extendedGroupProperties", SuggestedValuesBuilder.buildOpen(
                fetchExtensionAttributes(ObjectClass.GROUP) != null ?
                        fetchExtensionAttributes(ObjectClass.GROUP).toArray(new String[0]) : null
        ));

        suggestions.put("extendedSubjectProperties", SuggestedValuesBuilder.buildOpen(
                fetchExtensionAttributes(SubjectProcessing.O_CLASS) != null ?
                        fetchExtensionAttributes(SubjectProcessing.O_CLASS).toArray(new String[0]) : null
        ));

        return suggestions;
    }

    private Set<String> fetchExtensionAttributes(ObjectClass oClass) {
        LOG.info("Fetching extension attributes for the object class {0}", oClass);

        if (oClass.equals(ObjectClass.GROUP)) {

            GroupProcessing processing = new GroupProcessing(configuration);

            try {
                return processing.fetchExtensionSchema(connection.getConnection());

            } catch (SQLException e) {
                // TODO
                throw new RuntimeException(e);
            }

        } else if (oClass.equals(SubjectProcessing.O_CLASS)) {

            SubjectProcessing processing = new SubjectProcessing(configuration);

            try {
                return processing.fetchExtensionSchema(connection.getConnection());

            } catch (SQLException e) {
                // TODO
                throw new RuntimeException(e);
            }

        } else {

            throw new ConnectorException("Unexpected object class used in extension attribute evaluation.");
        }

    }
}
