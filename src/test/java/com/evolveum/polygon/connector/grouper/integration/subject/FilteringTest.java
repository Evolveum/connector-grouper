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

import com.evolveum.polygon.connector.grouper.integration.util.CommonTestClass;
import com.evolveum.polygon.connector.grouper.integration.util.TestSearchResultsHandler;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class FilteringTest extends CommonTestClass {

    @Test()
    public void fetchAll() {

        OperationOptions options = getDefaultOperationOptions(SUBJECT_NAME);
        ObjectClass objectClassSubject = new ObjectClass(SUBJECT_NAME);
        grouperConnector.init(grouperConfiguration);
        TestSearchResultsHandler handler = getResultHandler();

        grouperConnector.executeQuery(objectClassSubject, null, handler, options);
        ArrayList<ConnectorObject> results = handler.getResult();
    }
}
