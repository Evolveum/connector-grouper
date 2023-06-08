package com.evolveum.polygon.connector.grouper.util;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.ObjectClass;

import java.util.Map;

public class ResourceQuery {
    private static final Log LOG = Log.getLog(ResourceQuery.class);
    private ObjectClass objectClass;
    private Map<String, Map<String, Class>> columnInformation;

    private String query = null;

    public ResourceQuery(ObjectClass objectClass, Map<String, Map<String, Class>> columnInformation) {

        this.objectClass = objectClass;
        this.columnInformation = columnInformation;

    }

    public ObjectClass getObjectClass() {
        return objectClass;
    }

    public Map<String, Map<String, Class>> getColumnInformation() {
        return columnInformation;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

}
