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

import com.evolveum.polygon.connector.grouper.util.GroupProcessing;
import com.evolveum.polygon.connector.grouper.util.ObjectProcessing;
import org.identityconnectors.framework.common.objects.filter.*;
import util.CommonTestClass;
import util.TestSearchResultsHandler;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class FilteringTest extends CommonTestClass {

    private static final Log LOG = Log.getLog(FilteringTest.class);

    @Test()
    public void fetchAll() {

        LOG.ok("Execution of fetch all test for the Group object class");

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();

        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, null, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");
        }

    }

    @Test()
    public void fetchAllMaxPaging() {

        LOG.ok("Execution of fetch all test for the Group object class");

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME);

        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConfiguration.setEnableIdBasedPaging(true);
        grouperConfiguration.setMaxPageSize(2);

        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, null, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");
        }

    }

    @Test()
    public void fetchAllPagedCookie() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, false,
                "50", 0, 20);
        ObjectClass objectClassGroup = GroupProcessing.O_CLASS;
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.executeQuery(objectClassGroup, null, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");
        }
    }

    @Test()
    public void fetchAllPagedNoCookie() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, false,
                null, 1, 20);
        ObjectClass objectClassGroup = GroupProcessing.O_CLASS;
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.executeQuery(objectClassGroup, null, handler, options);
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

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, null, handler, options);
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

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EqualsFilter filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build(Uid.NAME,
                "34"));

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, filter, handler, options);
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

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EqualsFilter filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build(Uid.NAME,
                "34"));

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, filter, handler, options);
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
    public void equalsUIDAndAttributesToGetMaxPageSize() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConfiguration.setEnableIdBasedPaging(true);
        grouperConfiguration.setMaxPageSize(2);

        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EqualsFilter filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build(Uid.NAME,
                "34"));

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, filter, handler, options);
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
    public void andOrContainsUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter c1filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "4"));
        ContainsFilter c2filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "3"));
        ContainsFilter c3filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "34"));

        AndFilter a1Filter = (AndFilter) FilterBuilder.and(c1filter, c2filter);

        OrFilter orFilter = (OrFilter) FilterBuilder.or(a1Filter, c3filter);

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, orFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            if ((uid.contains("4") && uid.contains("3")) || uid.contains("34")) {

            } else {
                isOK = false;
            }


        }
        Assert.assertTrue(isOK);
    }

    @Test()
    public void andOrContainsUIDAndAttributesToGetWithPaging() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true,
                null, 0, 20);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter c1filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "4"));
        ContainsFilter c2filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "3"));
        ContainsFilter c3filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "34"));

        AndFilter a1Filter = (AndFilter) FilterBuilder.and(c1filter, c2filter);

        OrFilter orFilter = (OrFilter) FilterBuilder.or(a1Filter, c3filter);

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, orFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            if ((uid.contains("4") && uid.contains("3")) || uid.contains("34")) {

            } else {
                isOK = false;
            }


        }
        Assert.assertTrue(isOK);
    }

    @Test()
    public void andOrContainsUIDAndAttributesToGetWithPageCookie() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true,
                "15", 0, 20);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter c1filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "4"));
        ContainsFilter c2filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "3"));
        ContainsFilter c3filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "34"));

        AndFilter a1Filter = (AndFilter) FilterBuilder.and(c1filter, c2filter);

        OrFilter orFilter = (OrFilter) FilterBuilder.or(a1Filter, c3filter);

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, orFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            if ((uid.contains("4") && uid.contains("3")) || uid.contains("34")) {

            } else {
                isOK = false;
            }


        }
        Assert.assertTrue(isOK);
    }

    @Test()
    public void containsUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "4"));

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();

            if (uid.contains("4")) {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void containsUIDAndAttributesToGetAndPaging() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true,
                null, 0, 20);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "4"));

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();

            if (uid.contains("4")) {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void startsWithUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        StartsWithFilter filter = (StartsWithFilter) FilterBuilder.startsWith(AttributeBuilder.build(Uid.NAME,
                "4"));

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();

            if (uid.charAt(0) == '4') {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void endsWithUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EndsWithFilter filter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build(Uid.NAME,
                "4"));

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            int n = uid.length();

            if (uid.charAt(n - 1) == '4') {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void notEndsWithUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EndsWithFilter filter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build(Uid.NAME,
                "4"));

        NotFilter notFilter = (NotFilter) FilterBuilder.not(filter);

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, notFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            int n = uid.length();

            if (uid.charAt(n - 1) != '4') {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void endsWithStartsWithOrUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EndsWithFilter ewfilter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build(Uid.NAME,
                "4"));

        StartsWithFilter swfilter = (StartsWithFilter) FilterBuilder.startsWith(AttributeBuilder.build(Uid.NAME,
                "3"));

        OrFilter orFilter = (OrFilter) FilterBuilder.or(ewfilter, swfilter);

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, orFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            int n = uid.length();
            Boolean atStart = false;
            Boolean atEnd = false;

            if (uid.charAt(n - 1) == '4') {
                atStart = true;
            }

            if (uid.charAt(0) == '3') {
                atEnd = true;
            }
            isOK = atStart || atEnd;
        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void endsWithStartsWithANDUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EndsWithFilter ewfilter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build(Uid.NAME,
                "4"));

        StartsWithFilter swfilter = (StartsWithFilter) FilterBuilder.startsWith(AttributeBuilder.build(Uid.NAME,
                "3"));

        AndFilter andFilter = (AndFilter) FilterBuilder.and(ewfilter, swfilter);

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, andFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            int n = uid.length();

            if (uid.charAt(n - 1) == '4') {
            } else {
                isOK = false;
            }

            if (uid.charAt(0) == '3') {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void containsAllValues() {

        OperationOptions options = getDefaultOperationOptions(ObjectProcessing.GROUP_NAME, true);
        grouperConfiguration = initializeAndFetchGrouperConfiguration();
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsAllValuesFilter filter = (ContainsAllValuesFilter) FilterBuilder.containsAllValues(
                AttributeBuilder.build(ATTR_MEMBERS, "98"));

        grouperConnector.executeQuery(GroupProcessing.O_CLASS, filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");
        }
    }

}
