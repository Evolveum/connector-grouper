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

import com.evolveum.polygon.connector.grouper.GrouperConnection;
import org.identityconnectors.common.logging.Log;
import org.testng.annotations.Test;
import com.evolveum.polygon.connector.grouper.util.CommonTestClass;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Misc extends CommonTestClass {

    private static final Log LOG = Log.getLog(Misc.class);
    protected static final Integer MAX_IN = 100000;

    @Test()
    public void inHighVolume() {
        GrouperConnection grouperConnection = new GrouperConnection(grouperConfiguration);

        String query = "SELECT * FROM " + grouperConfiguration.getSchema() + "." + grouperConfiguration.getTablePrefix() + "_mp_subjects WHERE subject_id_index IN(";
        for (int i = 0; MAX_IN >= i; i++) {

            query = query + i;

            if (MAX_IN < i + 1) {
            } else {
                query = query + ", ";
            }
        }

        query = query + ") ORDER BY subject_id_index ASC";
        LOG.ok("The query: {0}", query);

        try {
            PreparedStatement prepareStatement = grouperConnection.getConnection().prepareStatement(query);
            prepareStatement.executeQuery();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
