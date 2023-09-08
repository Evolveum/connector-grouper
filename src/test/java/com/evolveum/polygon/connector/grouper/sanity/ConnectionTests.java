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

import com.evolveum.polygon.connector.grouper.util.CommonTestClass;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectionTests extends CommonTestClass {
    private static final Log LOG = Log.getLog(ConnectionTests.class);
    @Test()
    public void initializeCorrectConfigurationAndTestConnection() {
     grouperConnector.init(grouperConfiguration);
     grouperConnector.test();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void initializeNotCorrectConfigurationNullValuesAndTestConnection() {

        grouperConfiguration.setHost(null);
        grouperConfiguration.setPort(null);
        grouperConfiguration.validate();

        LOG.error("No exception yet, Code did not fail in validate method!");
        grouperConnector.init(grouperConfiguration);
        grouperConnector.test();
        Assert.fail();
    }

    @Test(expectedExceptions = ConnectionFailedException.class)
    public void initializeNotCorrectConfigurationValuesAndTestConnectionSelectFail() {
// Just a random IP
        grouperConfiguration.setHost("192.168.208.001");
        grouperConfiguration.setPort("27960");
        grouperConfiguration.validate();

        grouperConnector.init(grouperConfiguration);
        grouperConnector.test();
    }
}
