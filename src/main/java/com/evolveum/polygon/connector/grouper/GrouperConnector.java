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
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.TestOp;

import java.util.List;

@ConnectorClass(displayNameKey = "grouper.connector.display", configurationClass = GrouperConfiguration.class)
public class GrouperConnector implements Connector, SchemaOp, TestOp, SearchOp<Filter> {

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

        return translator.generateSchema();

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
                "opertaion options." : operationOptions);

        if (objectClass == null) {

            throw new IllegalArgumentException("Object class attribute can no be null");
        }

        if (resultsHandler == null) {
            LOG.error("Attribute of type ResultsHandler not provided.");
            throw new InvalidAttributeValueException("Attribute of type ResultsHandler is not provided.");
        }

        //TODO remove
        LOG.ok("1Test: {0}", objectClass.is(ObjectProcessing.SUBJECT_NAME));

        if (objectClass.is(ObjectProcessing.SUBJECT_NAME)) {
            SubjectProcessing subjectProcessing = new SubjectProcessing();

            LOG.ok("The object class for which the filter will be executed: {0}", objectClass.getDisplayNameKey());

            subjectProcessing.executeQuery(filter, resultsHandler, operationOptions, connection.getConnection());

        }
        LOG.ok("2Test: {0}", objectClass == ObjectClass.GROUP);
        if (objectClass == ObjectClass.GROUP) {
            GroupProcessing groupProcessing = new GroupProcessing();

            LOG.ok("The object class for which the filter will be executed: {0}", objectClass.getDisplayNameKey());

            groupProcessing.executeQuery(filter, resultsHandler, operationOptions, connection.getConnection());

        }
        LOG.ok("END The object class for which the filter will be executed: {0}, the subject object class:", objectClass.getDisplayNameKey());
    }

    @Override
    public void test() {
        LOG.info("Executing test operation.");
        connection.test();
        connection.dispose();

        LOG.ok("Test OK");
    }
}
