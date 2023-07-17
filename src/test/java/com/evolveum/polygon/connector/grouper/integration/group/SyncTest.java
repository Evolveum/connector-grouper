package com.evolveum.polygon.connector.grouper.integration.group;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.testng.Assert;
import org.testng.annotations.Test;
import util.CommonTestClass;
import util.TestSyncResultsHandler;

public class SyncTest extends CommonTestClass {
    private static final Log LOG = Log.getLog(SyncTest.class);

    @Test()
    public void syncTest() {


        OperationOptions options = getDefaultOperationOptions(ObjectClass.GROUP_NAME, true);
        ObjectClass objectClassGroup = new ObjectClass(ObjectClass.GROUP_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSyncResultsHandler handler = getSyncResultHandler();

        grouperConnector.sync(objectClassGroup, new SyncToken(1684824672269L),
                handler, options);


        for (SyncDelta result : handler.getResult()) {

            LOG.info("### START ### Attribute set for the object {0}", result);
            LOG.info("### END ###");
        }
    }

    @Test()
    public void latestSyncTokenTest() {


        OperationOptions options = getDefaultOperationOptions(ObjectClass.GROUP_NAME, true);
        ObjectClass objectClassGroup = new ObjectClass(ObjectClass.GROUP_NAME);
        grouperConnector.init(grouperConfiguration);


        SyncToken token = grouperConnector.getLatestSyncToken(objectClassGroup);

        LOG.ok("Sync token value : {0}", token);

        Assert.assertNotNull(token);
    }
}
