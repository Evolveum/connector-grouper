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

package com.evolveum.polygon.connector.grouper.integration.all;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.evolveum.polygon.connector.grouper.util.CommonTestClass;
import com.evolveum.polygon.connector.grouper.util.TestSyncResultsHandler;

public class SyncTest extends CommonTestClass {
    private static final Log LOG = Log.getLog(SyncTest.class);

    @Test()
    public void syncTest() {

        OperationOptions options = getDefaultOperationOptions(ObjectClass.ALL_NAME, true);
        ObjectClass objectClassALL = new ObjectClass(ObjectClass.ALL_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSyncResultsHandler handler = getSyncResultHandler();

        grouperConnector.sync(objectClassALL, new SyncToken(0L),
                handler, options);


        for (SyncDelta result : handler.getResult()) {

            LOG.info("### START ### Attribute set for the object {0}", result);
            LOG.info("### END ###");
        }

        Assert.assertTrue(!handler.getResult().isEmpty());
    }

    @Test()
    public void syncTestMaxPaging() {

        OperationOptions options = getDefaultOperationOptions(ObjectClass.ALL_NAME, true);
        ObjectClass objectClassALL = new ObjectClass(ObjectClass.ALL_NAME);
        grouperConfiguration.setEnableIdBasedPaging(true);
        grouperConfiguration.setMaxPageSize(4);
        grouperConnector.init(grouperConfiguration);
        TestSyncResultsHandler handler = getSyncResultHandler();

        grouperConnector.sync(objectClassALL, new SyncToken(0L),
                handler, options);


        for (SyncDelta result : handler.getResult()) {

            LOG.info("### START ### Attribute set for the object {0}", result);
            LOG.info("### END ###");
        }

        Assert.assertTrue(!handler.getResult().isEmpty());
    }

    @Test()
    public void latestSyncTokenTest() {

        ObjectClass objectClassGroup = new ObjectClass(ObjectClass.ALL_NAME);
        grouperConnector.init(grouperConfiguration);

        SyncToken token = grouperConnector.getLatestSyncToken(objectClassGroup);

        LOG.ok("Sync token value : {0}", token);

        Assert.assertNotNull(token.getValue());
    }
}
