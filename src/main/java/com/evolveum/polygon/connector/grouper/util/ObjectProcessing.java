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

package com.evolveum.polygon.connector.grouper.util;

import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public abstract class ObjectProcessing {

    public static final String SUBJECT_NAME = ObjectClassUtil.createSpecialName("SUBJECT");
    protected static final String ATTR_MODIFIED = "last_modified";

    // TODO MAP AS ACTIVATION ATTR?
    protected static final String ATTR_DELETED = "deleted";

    protected static final Map<String, Class> objectColumns = Map.ofEntries(
            Map.entry(ATTR_MODIFIED, BigInteger.class),
            Map.entry(ATTR_DELETED, char.class)
    );


    public abstract void buildObjectClass(SchemaBuilder schemaBuilder);

    public abstract void executeQuery(Filter filter, ResultsHandler handler, OperationOptions operationOptions
            , Connection connection);

    protected abstract boolean handleSqlObject(ResultSet resultSet, ResultsHandler handler) throws SQLException;
}
