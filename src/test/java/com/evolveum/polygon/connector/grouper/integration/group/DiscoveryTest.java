package com.evolveum.polygon.connector.grouper.integration.group;

import util.CommonTestClass;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.SuggestedValues;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class DiscoveryTest extends CommonTestClass {
    private static final Log LOG = Log.getLog(DiscoveryTest.class);

    @Test()
    public void discoverConfiguration() {

        LOG.ok("Execution of fetch all test for the Group object class");

        grouperConnector.init(grouperConfiguration);
        Map<String, SuggestedValues> discConfig = grouperConnector.discoverConfiguration();


        if (discConfig.containsKey("extendedGroupProperties")) {

            SuggestedValues sv = discConfig.get("extendedGroupProperties");

            for (Object obj : sv.getValues()) {

                if (obj instanceof Collection<?>) {

                    Iterator iter = ((Collection) obj).iterator();

                    while (iter.hasNext()) {
                        String str = (String) iter.next();
                        // TODO change based on dummy data
                        LOG.ok("The value of next: {0}, the class {1}", str, str.getClass());
                        if ("something".equals(str.toString())) {

                            Assert.assertTrue(true);
                        }
                    }
                }
            }
        } else {

           Assert.fail();
        }
    }

}
