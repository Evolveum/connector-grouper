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

package com.evolveum.polygon.connector.grouper.sanity;

import com.evolveum.polygon.connector.grouper.GrouperConfiguration;
import com.evolveum.polygon.connector.grouper.util.CommonTestClass;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

public class ConfigurationTests extends CommonTestClass {
    private static final Log LOG = Log.getLog(ConfigurationTests.class);

    @Test(expectedExceptions = ConfigurationException.class)
    public void initializeNotCorrectConfigurationNullValues() {

        grouperConfiguration.setHost(null);
        grouperConfiguration.setPort(null);
        grouperConfiguration.validate();

        Assert.fail();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void initializeNotCorrectConfigurationCombination() {

        grouperConfiguration.setEnableIdBasedPaging(false);
        grouperConfiguration.setMaxPageSize(2);
        grouperConfiguration.validate();

        Assert.fail();
    }

    @Test
    public void testGetSetTheProperties() {
        GrouperConfiguration testConfiguration = new GrouperConfiguration();

        testConfiguration.setHost("127.0.0.1");
        assertEquals("127.0.0.1", testConfiguration.getHost());
        testConfiguration.setPort("5432");
        assertEquals("5432", testConfiguration.getPort());
        testConfiguration.setUserName("midpoint");
        assertEquals("midpoint", testConfiguration.getUserName());
        testConfiguration.setDatabaseName("grouper");
        assertEquals("grouper", testConfiguration.getDatabaseName());
        testConfiguration.setConnectionValidTimeout(20);
        assertEquals(Integer.valueOf(20), testConfiguration.getConnectionValidTimeout());
        testConfiguration.setMaxPageSize(20);
        assertEquals(Integer.valueOf(20), testConfiguration.getMaxPageSize());

        String[] extendedGroupPropertiesn = new String[]{"something", "something1", "something2"};


        testConfiguration.setExtendedGroupProperties(new String[]{"something", "something1", "something2"});
        Assert.assertTrue(Arrays.equals(testConfiguration.getExtendedGroupProperties(), extendedGroupPropertiesn));

        String[] extendedSubjectPropertiesn = new String[]{"23456789", "12345678", "98764543", "A12345678"};
        testConfiguration.setExtendedSubjectProperties(new String[]{"23456789", "12345678", "98764543", "A12345678"});
        Assert.assertTrue(Arrays.equals(testConfiguration.getExtendedSubjectProperties(), extendedSubjectPropertiesn));

        testConfiguration.setExcludeDeletedObjects(false);
        assertEquals(Boolean.FALSE, testConfiguration.getExcludeDeletedObjects());

        testConfiguration.setEnableIdBasedPaging(true);
        assertEquals(Boolean.TRUE, testConfiguration.getEnableIdBasedPaging());
    }

    @Test
    public void testDefaultValues() {
        GrouperConfiguration testConfiguration = new GrouperConfiguration();

        assertEquals(Integer.valueOf(10), testConfiguration.getConnectionValidTimeout());
        assertEquals(Boolean.TRUE, testConfiguration.getExcludeDeletedObjects());
        assertEquals(Boolean.FALSE, testConfiguration.getEnableIdBasedPaging());
    }
}
