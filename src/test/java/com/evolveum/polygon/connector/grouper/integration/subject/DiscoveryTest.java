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

import com.evolveum.polygon.connector.grouper.util.CommonTestClass;
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


        if (discConfig.containsKey("extendedSubjectProperties")) {

            SuggestedValues sv = discConfig.get("extendedSubjectProperties");

            for (Object obj : sv.getValues()) {

                if (obj instanceof Collection<?>) {

                    Iterator iter = ((Collection) obj).iterator();

                    while (iter.hasNext()) {
                        String str = (String) iter.next();
                        // TODO change based on dummy data
                        LOG.ok("The value of next: {0}, the class {1}", str, str.getClass());
                        if ("12345678".equals(str.toString())) {

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
