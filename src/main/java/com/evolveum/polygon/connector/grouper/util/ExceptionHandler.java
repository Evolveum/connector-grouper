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

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.*;

import java.sql.SQLException;
import java.util.List;

public class ExceptionHandler {

    private static final Log LOG = Log.getLog(ExceptionHandler.class);

    public RuntimeException evaluateAndHandleException(Exception e, Boolean logErr, Boolean wrap,
                                                       String message) {

        if (e instanceof SQLException) {

            return handleBasedOnSQLState((SQLException) e, logErr, wrap, message);
        }

        LOG.ok("Default exception handling for ConnectorException.");
        return new ConnectorException(e);
    }

    private RuntimeException handleBasedOnSQLState(SQLException e, Boolean logErr, Boolean wrap,
                                                   String message) {
        String sqlState = e.getSQLState();

        List<String> connectionFailed = List.of("53300", "HV00N", "08000", "08003", "08006", "08001", "08004");
        List<String> invalidAttributeValue = List.of("HV024");

        List<String> configurationException = List.of("53400");
        List<String> connectionTimeOut = List.of("57P05", "25P03");
        List<String> permissionDenied = List.of("38004", "2F004");

        if (sqlState != null) {

            LOG.info("The SQLSTATE code of the processed SQL exception: {0}", sqlState);

            if (connectionFailed.contains(sqlState)) {

                LOG.ok("sqlState exception handling for ConnectionFailedException.");
                return checkIfLogAndReturn(new ConnectionFailedException(e), e,
                        logErr, message);

            } else if (invalidAttributeValue.contains(sqlState)) {

                LOG.ok("sqlState exception handling for InvalidAttributeValueException.");
                return checkIfLogAndReturn(new InvalidAttributeValueException(e), e,
                        logErr, message);

            } else if (configurationException.contains(sqlState)) {

                LOG.ok("sqlState exception handling for ConfigurationException.");
                return checkIfLogAndReturn(new ConfigurationException(e), e,
                        logErr, message);

            } else if (connectionTimeOut.contains(sqlState)) {

                LOG.ok("sqlState exception handling for ConfigurationException.");
                return checkIfLogAndReturn(new OperationTimeoutException(e), e,
                        logErr, message);

            } else if (permissionDenied.contains(sqlState)) {

                return checkIfLogAndReturn(new PermissionDeniedException(e), e,
                        logErr, message);

            }
        }

        // DEFAULT
        LOG.warn("Sql state code either null or not matched. Executing default exception handling.");

        if (!wrap) {

            LOG.ok("Default sqlState exception handling for ConnectorException");
            return checkIfLogAndReturn(new ConnectorException(e), e, logErr,
                    message);
        } else {

            LOG.ok("Default sqlState exception handling for ConnectorException, wrapping original sqlError.");
            return checkIfLogAndReturn(ConnectorException.wrap(e), e, logErr,
                    message);
        }

    }

    private RuntimeException checkIfLogAndReturn(RuntimeException exceptionToThrow, SQLException exceptionToConsume,
                                                 Boolean logErr, String errMessage) {

        String message = errMessage + " " + exceptionToConsume.getLocalizedMessage();
        if (!logErr) {

            LOG.ok(message);

        } else {
            LOG.error(message);
        }

        return exceptionToThrow;

    }
}
