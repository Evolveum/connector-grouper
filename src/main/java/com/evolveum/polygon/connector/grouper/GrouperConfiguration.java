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

import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.spi.ConfigurationProperty;
import org.identityconnectors.framework.spi.StatefulConfiguration;

import java.util.HashSet;

public class GrouperConfiguration extends AbstractConfiguration implements StatefulConfiguration {

    private static final Log LOG = Log.getLog(GrouperConfiguration.class);

    private Integer connectionValidTimeout = 10;
    private String databaseName;
    private GuardedString password;
    private String userName;
    private String port;
    private String host;
    private String[] extendedGroupProperties = {};
    private String[] extendedSubjectProperties = {};
    private Boolean excludeDeletedObjects = true;
    private Boolean enableIdBasedPaging = false;
    private Integer maxPageSize;

    @Override
    public void validate() {
        LOG.info("Execution of validate configuration method.");
        String messagePart = "One or more mandatory parameters are not set: ";
        HashSet<String> parameters = new HashSet<>();


        if (host != null && !host.isEmpty()) {
        } else {
            parameters.add("host");
        }

        if (port != null && !port.isEmpty()) {
        } else {
            parameters.add("port");
        }

        if (databaseName != null && !databaseName.isEmpty()) {
        } else {
            parameters.add("databaseName");
        }

        if (password != null) {
        } else {
            parameters.add("password");
        }

        if (userName != null && !userName.isEmpty()) {
        } else {
            parameters.add("loginName");
        }

        if (connectionValidTimeout == 10) {

            LOG.info("Connection validation timeout will be used with the value 10 seconds for connection" +
                    "validation.");
        }

        if (excludeDeletedObjects == true) {

            LOG.info("Deleted object 'rows' are excluded from object lookup and searches. Any object (or related row)" +
                    "marked as 'deleted=T' will be omitted from the result set.");
        }


        if (!parameters.isEmpty()) {

            throw new ConfigurationException(messagePart + parameters);
        }
    }

    @ConfigurationProperty(order = 2,
            displayMessageKey = "host.display",
            helpMessageKey = "host.help",
            required = true)

    public String getHost() {
        return this.host;
    }

    public void setHost(String value) {
        this.host = value;
    }

    @ConfigurationProperty(order = 3,
            displayMessageKey = "port.display",
            helpMessageKey = "port.help", required = true)
    public String getPort() {
        return this.port;
    }

    public void setPort(String value) {
        this.port = value;
    }


    @ConfigurationProperty(order = 4,
            displayMessageKey = "userName.display",
            helpMessageKey = "userName.help",
            required = true)
    public String getUserName() {
        return this.userName;
    }

    public void setUserName(String value) {
        this.userName = value;
    }

    @ConfigurationProperty(order = 5, confidential = true,
            displayMessageKey = "password.display",
            helpMessageKey = "password.help",
            required = true)
    public GuardedString getPassword() {
        return this.password;
    }

    public void setPassword(GuardedString value) {
        this.password = value;
    }


    @ConfigurationProperty(order = 6,
            displayMessageKey = "databaseName.display",
            helpMessageKey = "databaseName.help",
            required = true)
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }


    @ConfigurationProperty(order = 7,
            displayMessageKey = "connectionValidTimeout.display",
            helpMessageKey = "connectionValidTimeout.help")
    public Integer getConnectionValidTimeout() {
        return connectionValidTimeout;
    }

    public void setConnectionValidTimeout(Integer connectionValidTimeout) {
        this.connectionValidTimeout = connectionValidTimeout;
    }

    @ConfigurationProperty(order = 8, displayMessageKey = "extendedGroupProperties.display",
            helpMessageKey = "extendedGroupProperties.help")

    public String[] getExtendedGroupProperties() {
        return extendedGroupProperties;
    }

    public void setExtendedGroupProperties(String[] extendedGroupProperties) {
        this.extendedGroupProperties = extendedGroupProperties;
    }

    @ConfigurationProperty(order = 9, displayMessageKey = "extendedSubjectProperties.display",
            helpMessageKey = "extendedSubjectProperties.help")

    public String[] getExtendedSubjectProperties() {
        return extendedSubjectProperties;
    }

    public void setExtendedSubjectProperties(String[] extendedSubjectProperties) {
        this.extendedSubjectProperties = extendedSubjectProperties;
    }

    @ConfigurationProperty(order = 10, displayMessageKey = "excludeDeletedObjects.display",
            helpMessageKey = "excludeDeletedObjects.help")

    public Boolean getExcludeDeletedObjects() {
        return excludeDeletedObjects;
    }

    public void setExcludeDeletedObjects(Boolean excludeDeletedObjects) {
        this.excludeDeletedObjects = excludeDeletedObjects;
    }

    @ConfigurationProperty(order = 11, displayMessageKey = "enableIdBasedPaging.display",
            helpMessageKey = "enableIdBasedPaging.help")

    public Boolean getEnableIdBasedPaging() {
        return enableIdBasedPaging;
    }

    public void setEnableIdBasedPaging(Boolean enableIdBasedPaging) {
        this.enableIdBasedPaging = enableIdBasedPaging;
    }

    @ConfigurationProperty(order = 12, displayMessageKey = "maxPageSize.display",
            helpMessageKey = "maxPageSize.help")

    public Integer getMaxPageSize() {
        return maxPageSize;
    }

    public void setMaxPageSize(Integer maxPageSize) {
        this.maxPageSize = maxPageSize;
    }


    @Override
    public void release() {

        connectionValidTimeout = null;
        databaseName = null;
        password.dispose();
        userName = null;
        port = null;
        host = null;
        extendedSubjectProperties = null;
        extendedGroupProperties = null;
        excludeDeletedObjects = true;
        maxPageSize = null;
    }
}