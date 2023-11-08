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
import org.identityconnectors.common.security.GuardedString;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class PropertiesParser {

    private static final Log LOGGER = Log.getLog(PropertiesParser.class);

    private static final Properties PROPERTIES = new Properties();

    private static final String PROPERTIES_PATH = "/testProperties/propertiesForTest.properties";
    private static final String _HOST = "host";
    private static final String _PORT = "port";
    private static final String _LOGIN = "userName";
    private static final String _PASSWORD = "password";
    private static final String _DATABASE = "databaseName";
    private static final String _VALID_TIMEOUT = "connectionValidTimeout";
    private static final String _EXTENDED_PROPERTIES_SUBJECT = "extendedSubjectProperties";
    private static final String _EXTENDED_PROPERTIES_GROUP = "extendedGroupProperties";
    private static final String _ENABLE_ID_BASED_PAGING = "enableIdBasedPaging";
    private static final String _EXCLUDE_DELETED = "excludeDeletedObjects";
    private static final String _MAX_PAGE_SIZE = "maxPageSize";
    private static final String _ATTRS_ALL_SEARCH = "attrsToHaveInAllSearch";
    private static final String _TABLE_PREFIX = "tablePrefix";


    public PropertiesParser() {

        try {
            PROPERTIES.load(getClass().getResourceAsStream(PROPERTIES_PATH));
        } catch (FileNotFoundException e) {
            LOGGER.error(e, "File not found: {0}", e.getLocalizedMessage());
        } catch (IOException e) {
            LOGGER.error(e, "IO exception occurred {0}", e.getLocalizedMessage());
        } catch (NullPointerException e) {
            LOGGER.error(e, "Properties file not found", e.getLocalizedMessage());
        }
    }

    public String getHost() {
        return (String) PROPERTIES.get(_HOST);
    }

    public String getPort() {
        return (String) PROPERTIES.get(_PORT);
    }

    public String getLogin() {
        return (String) PROPERTIES.get(_LOGIN);
    }

    public GuardedString getPassword() {
        return new GuardedString(((String) PROPERTIES.get(_PASSWORD)).toCharArray());
    }

    public String getDatabase() {
        return (String) PROPERTIES.get(_DATABASE);
    }
    //TODO
    public String getTablePrefix() {
        return (String) PROPERTIES.get(_TABLE_PREFIX);
    }

    public Set<String> getSubjectProperties() {
        return getValues(_EXTENDED_PROPERTIES_SUBJECT);
    }

    public Set<String> getGroupProperties() {
        return getValues(_EXTENDED_PROPERTIES_GROUP);
    }

    public Boolean getEnableIdBasedPaging() {

        return "true".equalsIgnoreCase((String) PROPERTIES.get(_ENABLE_ID_BASED_PAGING));
    }

    public Boolean getExcludeDeletedObjects() {

        return "true".equalsIgnoreCase((String) PROPERTIES.get(_EXCLUDE_DELETED));
    }

    public Integer getValidTimeout() {
        String timeout = (String) PROPERTIES.get(_VALID_TIMEOUT);

        if (timeout.isBlank()) {

            return null;
        }

        return Integer.parseInt(timeout);
    }

    private Set<String> getValues(String name) {
        Set<String> values = new HashSet<>();

        if (PROPERTIES.containsKey(name)) {
            String value = (String) PROPERTIES.get(name);
            values.addAll(Arrays.asList(value.split(",")));
        }

        return values;
    }

    public Integer getMaxPageSize() {
        String maxSize = (String) PROPERTIES.get(_MAX_PAGE_SIZE);

        if (maxSize.isBlank()) {

            return null;
        }

        return Integer.parseInt(maxSize);
    }

    public Set<String> getAttrsAllSearch() {
        return getValues(_ATTRS_ALL_SEARCH);
    }

}