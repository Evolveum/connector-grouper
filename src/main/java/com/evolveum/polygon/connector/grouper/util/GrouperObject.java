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

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class GrouperObject {

    private static final Log LOG = Log.getLog(GrouperObject.class);
    private String identifier;
    private String name;
    public Map<String, Object> attributes = new HashMap<>();
    public Boolean deleted = false;

    public GrouperObject() {
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public void addAttribute(String name, Object value, Set<String> multiValuedAttributesCatalogue) {

        //TODO remove log
        LOG.ok("X- Attribute being processed: {0}", name);

        if (attributes.containsKey(name)) {

            if (attributes.get(name) instanceof Set<?>) {

                Set<Object> multivalSet = (Set<Object>) attributes.get(name);

                multivalSet.add(value);
                //TODO remove log
                multivalSet.forEach(val -> LOG.ok("@ Current values present  in the set: {0}", val));
                LOG.ok("Attribute value added: {0}", value);
                attributes.put(name, multivalSet);
            } else {

                attributes.put(name, value);
            }

        } else {
            if (multiValuedAttributesCatalogue.contains(name)) {
                Set<Object> multivalSet = new HashSet<>();
                multivalSet.add(value);
                attributes.put(name, multivalSet);
            } else {

                attributes.put(name, value);

            }
        }
    }

    @Override
    public String toString() {

        String str = "Identifier: " + identifier
                + "; " + "Name: " + name + "; Attributes:{ ";

        Iterator keyIterator = attributes.keySet().iterator();

        while (keyIterator.hasNext()) {

            String attrName = (String) keyIterator.next();
            Object obj = attributes.get(attrName);

            if (obj instanceof Set<?>) {
                Iterator iterator = ((Set<?>) obj).iterator();

                while (iterator.hasNext()) {
                    Object object = iterator.next();

                    if (!iterator.hasNext()) {

                        str = str + attrName + ": " + object;
                    } else {

                        str = str + attrName + ": " + object + "; ";
                    }
                }
            } else {
                if (!keyIterator.hasNext()) {

                    str = str + attrName + ": " + attributes.get(attrName);
                } else {

                    str = str + attrName + ": " + attributes.get(attrName) + "; ";
                }
            }

        }

        str = str + " }";

        return str.toString();
    }

}
