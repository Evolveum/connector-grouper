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
package com.evolveum.polygon.connector.grouper.unit;

import com.evolveum.polygon.connector.grouper.util.ObjectProcessing;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.evolveum.polygon.connector.grouper.util.CommonTestClass;

import java.util.Iterator;
import java.util.Set;

public class SchemaTests extends CommonTestClass {
    private static final Log LOG = Log.getLog(SchemaTests.class);

    @Test()
    public void parseSchemaTest() {
        grouperConnector.init(grouperConfiguration);
        Schema schema = grouperConnector.schema();

        Iterator<ObjectClassInfo> iterator = schema.getObjectClassInfo().iterator();
        Set groupExtProps = parser.getGroupProperties();
        Set subjExtProps = parser.getSubjectProperties();

        Boolean hasGroupExtensions = false;
        Boolean hasSubjectExtensions = false;


        while (iterator.hasNext()) {
            ObjectClassInfo oclass = iterator.next();

            LOG.ok("The object class info: {0}", oclass);


            if (oclass.is(ObjectProcessing.GROUP_NAME)) {
                Iterator<AttributeInfo> ait = oclass.getAttributeInfo().iterator();
                while (ait.hasNext()) {
                    AttributeInfo aif = ait.next();
                    if (groupExtProps != null && !groupExtProps.isEmpty()) {

                        if (groupExtProps.contains(aif.getName())) {

                            hasGroupExtensions = true;
                            break;
                        }

                    } else {

                        throw new ConfigurationException("Test failed due to empty group extension" +
                                " properties field, please check properties file");
                    }

                }
//            } else if (oclass.is(SUBJECT_NAME)) {

            } else  if (oclass.is(ObjectProcessing.SUBJECT_NAME)) {
                Iterator<AttributeInfo> ait = oclass.getAttributeInfo().iterator();

                while (ait.hasNext()) {
                    AttributeInfo aif = ait.next();
                    if (subjExtProps != null && !subjExtProps.isEmpty()) {

                        if (subjExtProps.contains(aif.getName())) {

                            hasSubjectExtensions = true;
                            break;
                        }

                    } else {

                        throw new ConfigurationException("Test failed due to empty subject extension" +
                                " properties field, please check properties file");
                    }
                }
            }
        }

        Assert.assertTrue(hasGroupExtensions && hasSubjectExtensions);
    }
}
