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

package com.evolveum.polygon.connector.grouper.integration.subject;

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

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME);
        ObjectClass objectClassSubject = new ObjectClass(SUBJECT_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.executeQuery(objectClassSubject, null, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");
        }
    }


    //TODO test
    @Test()
    public void fetchAllPagedCookie() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, false, "87",
                0, 20);
        ObjectClass objectClassSubject = new ObjectClass(SUBJECT_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.executeQuery(objectClassSubject, null, handler, options);
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

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, false, null,
                0, 20);
        ObjectClass objectClassSubject = new ObjectClass(SUBJECT_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.executeQuery(objectClassSubject, null, handler, options);
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

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        ObjectClass objectClassSubject = new ObjectClass(SUBJECT_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.executeQuery(objectClassSubject, null, handler, options);
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

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME);
        ObjectClass objectClassSubject = new ObjectClass(SUBJECT_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EqualsFilter filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build(Uid.NAME,
                "82"));

        grouperConnector.executeQuery(objectClassSubject, filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            Assert.assertEquals(result.getUid().getUidValue(), "82");
        }
    }

    @Test()
    public void equalsUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME,
                true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EqualsFilter filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build(Uid.NAME,
                "103"));

        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            Assert.assertEquals(result.getUid().getUidValue(), "103");
        }
    }

    @Test()
    public void containsUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "1"));

        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();

            if (uid.contains("1")) {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void containsUIDAndAttributesToGetWithPaging() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true,
                null, 0, 20);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "1"));

        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();

            if (uid.contains("1")) {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }


    @Test()
    public void startsWithUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        StartsWithFilter filter = (StartsWithFilter) FilterBuilder.startsWith(AttributeBuilder.build(Uid.NAME,
                "1"));

        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();

            if (uid.charAt(0) == '1') {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void endsWithUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EndsWithFilter filter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build(Uid.NAME,
                "7"));

        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            int n = uid.length();

            if (uid.charAt(n - 1) == '7') {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void notEndsWithUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EndsWithFilter filter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build(Uid.NAME,
                "2"));

        NotFilter notFilter = (NotFilter) FilterBuilder.not(filter);

        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), notFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            int n = uid.length();

            if (uid.charAt(n - 1) != '2') {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void endsWithStartsWithOrUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EndsWithFilter ewfilter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build(Uid.NAME,
                "7"));

        StartsWithFilter swfilter = (StartsWithFilter) FilterBuilder.startsWith(AttributeBuilder.build(Uid.NAME,
                "8"));

        OrFilter orFilter = (OrFilter) FilterBuilder.or(ewfilter, swfilter);

        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), orFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            int n = uid.length();

            if (uid.charAt(n - 1) == '7') {
            } else {
                isOK = false;
            }

            if (uid.charAt(0) == '8') {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void endsWithStartsWithANDUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        EndsWithFilter ewfilter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build(Uid.NAME,
                "7"));

        StartsWithFilter swfilter = (StartsWithFilter) FilterBuilder.startsWith(AttributeBuilder.build(Uid.NAME,
                "8"));

        AndFilter andFilter = (AndFilter) FilterBuilder.and(ewfilter, swfilter);

        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), andFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            int n = uid.length();

            if (uid.charAt(n - 1) == '7') {
            } else {
                isOK = false;
            }

            if (uid.charAt(0) == '8') {
            } else {
                isOK = false;
            }

        }

        Assert.assertTrue(isOK);
    }

    @Test()
    public void andOrContainsUIDAndAttributesToGet() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);

        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter c1filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "1"));
        ContainsFilter c2filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "0"));
        ContainsFilter c3filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "87"));

        AndFilter a1Filter = (AndFilter) FilterBuilder.and(c1filter, c2filter);

        OrFilter orFilter = (OrFilter) FilterBuilder.or(a1Filter, c3filter);


        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), orFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            if ((uid.contains("1") && uid.contains("0")) || uid.contains("87")) {

            } else {
                isOK = false;
            }

        }
        Assert.assertTrue(isOK);
    }

    @Test()
    public void andOrContainsUIDAndAttributesToGetWithPaging() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true, null,
                0, 20);

        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter c1filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "1"));
        ContainsFilter c2filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "0"));
        ContainsFilter c3filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "87"));

        AndFilter a1Filter = (AndFilter) FilterBuilder.and(c1filter, c2filter);

        OrFilter orFilter = (OrFilter) FilterBuilder.or(a1Filter, c3filter);


        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), orFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            if ((uid.contains("1") && uid.contains("0")) || uid.contains("87")) {

            } else {
                isOK = false;
            }

        }
        Assert.assertTrue(isOK);
    }

    @Test()
    public void andOrContainsUIDAndAttributesToGetWithPageCookie() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true, "10",
                0, 20);

        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsFilter c1filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "1"));
        ContainsFilter c2filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "0"));
        ContainsFilter c3filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build(Uid.NAME,
                "87"));

        AndFilter a1Filter = (AndFilter) FilterBuilder.and(c1filter, c2filter);

        OrFilter orFilter = (OrFilter) FilterBuilder.or(a1Filter, c3filter);


        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), orFilter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();

        boolean isOK = true;
        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");

            String uid = result.getUid().getUidValue();
            if ((uid.contains("1") && uid.contains("0")) || uid.contains("87")) {

            } else {
                isOK = false;
            }

        }
        Assert.assertTrue(isOK);
    }

    @Test()
    public void containsAllValues() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        ContainsAllValuesFilter filter = (ContainsAllValuesFilter) FilterBuilder.containsAllValues(
                AttributeBuilder.build(ATTR_MEMBER_OF, "34"));

        grouperConnector.executeQuery(new ObjectClass(SUBJECT_NAME), filter, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();


        for (ConnectorObject result : results) {

            LOG.info("### START ### Attribute set for the object {0}", result.getName());
            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
                    obj.getName(), obj.getValue()));
            LOG.info("### END ###");
        }
    }
}
