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

package com.evolveum.polygon.connector.grouper;

import com.evolveum.polygon.common.GuardedStringAccessor;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class GrouperConnection {
    private static final Log LOG = Log.getLog(GrouperConnection.class);

    private GrouperConfiguration configuration;
    private Connection connection;

    public GrouperConnection(GrouperConfiguration configuration) {
        this.configuration = configuration;

        connection = initialize(configuration);
    }

    private static Connection initialize(GrouperConfiguration configuration) {
        final PGConnectionPoolDataSource dataSource = new PGConnectionPoolDataSource();
        Connection connection;
        dataSource.setPortNumbers(new int[Integer.parseInt(configuration.getPort())]);
        dataSource.setUser(configuration.getUserName());
        dataSource.setServerName(configuration.getHost());
        dataSource.setDatabaseName(configuration.getDatabaseName());

        GuardedString clientPassword = configuration.getPassword();
        GuardedStringAccessor accessorSecret = new GuardedStringAccessor();
        clientPassword.access(accessorSecret);

        dataSource.setPassword(accessorSecret.getClearString());


        try {
            connection = dataSource.getConnection();

        } catch (SQLException e) {

            throw new ConnectionFailedException("Database connection could not be established by the connector: "
                    + e.getLocalizedMessage());
        }

        //TODO do we need to disable auto commit? With read only ?

        return connection;
    }

    public void test() {
        LOG.ok("Testing connection via psql validation method");
        try {

            if (!connection.isValid(configuration.getConnectionValidTimeout())) {

                throw new ConnectionFailedException("The connection validation method evaluated the connection as " +
                        "not valid.");
            }

            Statement statement = null;
            statement = connection.createStatement();

            if (!statement.execute("SELECT 1;")) {

                throw new ConnectionFailedException("Connection not valid per SQL statement validation.");
            }

        } catch (SQLException e) {

            throw new ConnectorException("Error while evaluating connection validity " + e.getLocalizedMessage());
        }
    }

    public void dispose() {
        configuration = null;

        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new ConnectorException("Exception while closing a connection to the resource database: "
                    + e.getLocalizedMessage());
        }
    }

    public Connection getConnection() {
        return connection;
    }
}