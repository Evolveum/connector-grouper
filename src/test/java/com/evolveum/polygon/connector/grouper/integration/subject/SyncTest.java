package com.evolveum.polygon.connector.grouper.integration.subject;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.*;
import org.testng.Assert;
import org.testng.annotations.Test;
import util.CommonTestClass;
import util.TestSyncResultsHandler;

public class SyncTest extends CommonTestClass {
    private static final Log LOG = Log.getLog(SyncTest.class);

    @Test()
    public void syncTest() {

        //OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME);
        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        ObjectClass objectClassSubject = new ObjectClass(SUBJECT_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSyncResultsHandler handler = getSyncResultHandler();

        grouperConnector.sync(objectClassSubject, new SyncToken(1686733562L),
                handler, options);


        for (SyncDelta result : handler.getResult()) {

            LOG.info("### START ### Attribute set for the object {0}", result);
            LOG.info("### END ###");
        }
    }

    @Test()
    public void latestSyncTokenTest() {


        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        ObjectClass objectClassGroup = new ObjectClass(SUBJECT_NAME);
        grouperConnector.init(grouperConfiguration);


        SyncToken token = grouperConnector.getLatestSyncToken(objectClassGroup);

        LOG.ok("Sync token value : {0}", token);

        Assert.assertNotNull(token);
    }
}
