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

package com.evolveum.polygon.connector.grouper.integration.group;

import util.CommonTestClass;
import util.TestSearchResultsHandler;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.ContainsAllValuesFilter;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class FilteringTest extends CommonTestClass {

    private static final Log LOG = Log.getLog(FilteringTest.class);

    @Test()
    public void fetchAll() {

        LOG.ok("Execution of fetch all test for the Group object class");

        OperationOptions options = getDefaultOperationOptions(ObjectClass.GROUP_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getResultHandler();

        grouperConnector.executeQuery(ObjectClass.GROUP, null, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");
        }

    }

    @Test()
    public void fetchAllWithAttrsToGet() {

        LOG.ok("Execution of fetch all test for the Group object class");

        OperationOptions options = getDefaultOperationOptions(ObjectClass.GROUP_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getResultHandler();

        grouperConnector.executeQuery(ObjectClass.GROUP, null, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");
        }

    }

    @Test()
    public void equalsUID() {

        OperationOptions options = getDefaultOperationOptions(ObjectClass.GROUP_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getResultHandler();

        EqualsFilter filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build(Uid.NAME,
                "34"));

        grouperConnector.executeQuery(ObjectClass.GROUP, filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            Assert.assertEquals(result.getUid().getUidValue(), "34");
        }
    }

    @Test()
    public void equalsUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(ObjectClass.GROUP_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getResultHandler();

        EqualsFilter filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build(Uid.NAME,
                "45"));

        grouperConnector.executeQuery(ObjectClass.GROUP, filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            Assert.assertEquals(result.getUid().getUidValue(), "45");
        }
    }

    @Test()
    public void containsAllValues() {

        OperationOptions options = getDefaultOperationOptions(ObjectClass.GROUP_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getResultHandler();

        ContainsAllValuesFilter filter = (ContainsAllValuesFilter) FilterBuilder.containsAllValues(
                AttributeBuilder.build(ATTR_MEMBERS, "98"));

        grouperConnector.executeQuery(ObjectClass.GROUP, filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");
        }
    }

}
