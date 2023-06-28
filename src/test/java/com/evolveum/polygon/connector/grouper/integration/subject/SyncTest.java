package com.evolveum.polygon.connector.grouper.integration.subject;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.*;
import org.testng.annotations.Test;
import util.CommonTestClass;
import util.TestSearchResultsHandler;

public class SyncTest extends CommonTestClass {
    private static final Log LOG = Log.getLog(SyncTest.class);

    @Test()
    public void syncTest() {

        //OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME);
        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME, true);
        ObjectClass objectClassSubject = new ObjectClass(SUBJECT_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getSearchResultHandler();

        grouperConnector.sync(objectClassSubject, new SyncToken("1686733562"),
                (SyncResultsHandler) getSyncResultHandler(), options);


//        for (ConnectorObject result : results) {
//
//            LOG.info("### START ### Attribute set for the object {0}", result.getName());
//            result.getAttributes().forEach(obj -> LOG.info("The attribute: {0}, with value {1}",
//                    obj.getName(), obj.getValue()));
//            LOG.info("### END ###");
//        }
    }
}
