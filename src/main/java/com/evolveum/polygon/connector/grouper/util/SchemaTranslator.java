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
import com.evolveum.polygon.connector.grouper.GrouperConnector;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.OperationOptionInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.spi.operations.SearchOp;

import java.util.HashSet;
import java.util.Set;

public class SchemaTranslator {
    private static final Log LOG = Log.getLog(SchemaTranslator.class);

    public Schema generateSchema(GrouperConfiguration configuration) {
        LOG.info("Generating schema object");

        SchemaBuilder schemaBuilder = new SchemaBuilder(GrouperConnector.class);
        GroupProcessing groupProcessing = new GroupProcessing(configuration);
        SubjectProcessing userProcessing = new SubjectProcessing(configuration);

        groupProcessing.buildObjectClass(schemaBuilder, configuration);
        userProcessing.buildObjectClass(schemaBuilder, configuration);

        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildAttributesToGet(), SearchOp.class);
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildReturnDefaultAttributes(), SearchOp.class);

//TODO test with schema refresh
        if (configuration.getEnableIdBasedPaging()) {

            schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildPageSize());
            schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildPagedResultsOffset());
            schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildPagedResultsCookie());
        }

        return schemaBuilder.build();
    }
}
