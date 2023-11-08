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
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.*;

public class CommonTestClass implements ObjectConstants {

    private static final Log LOG = Log.getLog(CommonTestClass.class);
    protected final PropertiesParser parser = new PropertiesParser();
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
        grouperConfiguration.setEnableIdBasedPaging(parser.getEnableIdBasedPaging());
        //TODO
        grouperConfiguration.setTablePrefix(parser.getTablePrefix());
        grouperConfiguration.setExcludeDeletedObjects(parser.getExcludeDeletedObjects());

        if (parser.getValidTimeout() != null) {
            grouperConfiguration.setConnectionValidTimeout(parser.getValidTimeout());
        }

        if (parser.getGroupProperties() != null) {
            grouperConfiguration.setExtendedGroupProperties(parser.getGroupProperties().toArray(new String[0]));
        }

        if (parser.getSubjectProperties() != null) {
            grouperConfiguration.setExtendedSubjectProperties(parser.getSubjectProperties().toArray(new String[0]));
        }

        if (parser.getAttrsAllSearch() != null) {
            grouperConfiguration.setAttrsToHaveInAllSearch(parser.getAttrsAllSearch().toArray(new String[0]));
        }

        grouperConfiguration.validate();
        return grouperConfiguration;
    }

    protected TestSearchResultsHandler getSearchResultHandler() {

        return new TestSearchResultsHandler();
    }

    protected TestSyncResultsHandler getSyncResultHandler() {

        return new TestSyncResultsHandler();
    }


    protected OperationOptions getDefaultOperationOptions(String objectClassName) {

        return getDefaultOperationOptions(objectClassName, false, null,
                null, null);
    }

    protected OperationOptions getDefaultOperationOptions(String objectClassName, Boolean extendedAttrsToGet) {

        return getDefaultOperationOptions(objectClassName, extendedAttrsToGet, null,
                null, null);
    }

    protected OperationOptions getDefaultOperationOptions(String objectClassName, Boolean extendedAttrsToGet
            , String pageCookie, Integer pageOffset, Integer pageSize) {

        List<String> groupArray = CollectionUtil.newList(ATTR_NAME, ATTR_DISPLAY_NAME,
                ATTR_DESCRIPTION);
        List<String> subjectArray = CollectionUtil.newList(ATTR_ID);

        Map<String, Object> operationOptions = new HashMap<>();

        if (ObjectProcessing.GROUP_NAME.equals(objectClassName)) {
            if (extendedAttrsToGet) {

                groupArray.add(ATTR_MEMBERS);


                if (grouperConfiguration.getExtendedGroupProperties() != null) {

                    groupArray.addAll(Arrays.asList(grouperConfiguration.getExtendedGroupProperties()));
                }

            }

            operationOptions.put(OperationOptions.OP_ATTRIBUTES_TO_GET, groupArray.toArray(new String[0]));

        } else if (ObjectProcessing.SUBJECT_NAME.equals(objectClassName)) {
            if (extendedAttrsToGet) {

                subjectArray.add(ATTR_MEMBER_OF);

                if (grouperConfiguration.getExtendedSubjectProperties() != null) {

                    subjectArray.addAll(Arrays.asList(grouperConfiguration.getExtendedSubjectProperties()));
                }
            }

            operationOptions.put(OperationOptions.OP_ATTRIBUTES_TO_GET, subjectArray.toArray(new String[0]));
        }

        if (pageOffset != null) {

            operationOptions.put(OperationOptions.OP_PAGED_RESULTS_OFFSET, pageOffset);
        }

        if (pageSize != null) {

            operationOptions.put(OperationOptions.OP_PAGE_SIZE, pageSize);
        }

        if (pageCookie != null) {

            operationOptions.put(OperationOptions.OP_PAGED_RESULTS_COOKIE, pageCookie);
        }

        OperationOptions options = new OperationOptions(operationOptions);

        return options;
    }

    @BeforeMethod
    protected void init() {
        grouperConnector = new GrouperConnector();
        initializeAndFetchGrouperConfiguration();
    }

    @AfterMethod
    protected void cleanup() {
        grouperConnector.dispose();
        grouperConfiguration.release();
    }

}
