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

package com.evolveum.polygon.connector.grouper.integration.util;

import com.evolveum.polygon.connector.grouper.GrouperConfiguration;
import com.evolveum.polygon.connector.grouper.GrouperConnector;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.*;

public class CommonTestClass implements ObjectConstants {

    private final PropertiesParser parser = new PropertiesParser();
    ;
    protected GrouperConfiguration grouperConfiguration = new GrouperConfiguration();
    protected GrouperConnector grouperConnector = new GrouperConnector();

    public CommonTestClass() {
        init();
    }

    protected GrouperConfiguration initializeAndFetchGrouperConfiguration() {

        grouperConfiguration = new GrouperConfiguration();

        grouperConfiguration.setHost(parser.getHost());
        grouperConfiguration.setDatabaseName(parser.getDatabase());
        grouperConfiguration.setPort(parser.getPort());
        grouperConfiguration.setPassword(parser.getPassword());
        grouperConfiguration.setUserName(parser.getLogin());
        grouperConfiguration.setConnectionValidTimeout(parser.getValidTimeout());

        return grouperConfiguration;
    }

    protected TestSearchResultsHandler getResultHandler() {

        return new TestSearchResultsHandler();
    }

    protected OperationOptions getDefaultOperationOptions(String objectClassName) {

        return getDefaultOperationOptions(objectClassName, false);
    }

    protected OperationOptions getDefaultOperationOptions(String objectClassName, Boolean extendedAttrsToGet) {

        List<String> groupArray = CollectionUtil.newList(ATTR_NAME, ATTR_DISPLAY_NAME,
                ATTR_DESCRIPTION);
        List<String> subjectArray = CollectionUtil.newList(ATTR_ID);

        Map<String, Object> operationOptions = new HashMap<>();
        //TODO
        if (ObjectClass.GROUP_NAME.equals(objectClassName)) {
            if (extendedAttrsToGet) {

                groupArray.add(ATTR_MEMBERS);
            }

            operationOptions.put(OperationOptions.OP_ATTRIBUTES_TO_GET, groupArray.toArray(new String[0]));
        } else {
            if (extendedAttrsToGet) {
                subjectArray.add(ATTR_MEMBER_OF);
            }

            operationOptions.put(OperationOptions.OP_ATTRIBUTES_TO_GET, subjectArray.toArray(new String[0]));
        }
        OperationOptions options = new OperationOptions(operationOptions);

        return options;
    }

    @BeforeMethod
    private void init() {
        grouperConnector = new GrouperConnector();
        initializeAndFetchGrouperConfiguration();
    }

    @AfterMethod
    private void cleanup() {
        grouperConnector.dispose();
        grouperConfiguration.release();
    }

}
