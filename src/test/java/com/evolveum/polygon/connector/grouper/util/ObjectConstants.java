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

import com.evolveum.polygon.connector.grouper.util.ObjectProcessing;
import org.identityconnectors.framework.common.objects.ObjectClassUtil;

public interface ObjectConstants {

    // GROUP OBJECT CLASS
    String ATTR_NAME = "name";
    String ATTR_DISPLAY_NAME = "display_name";
    String ATTR_DESCRIPTION = "description";
    String ATTR_MEMBERS = "members";

    // SUBJECT OBJECT CLASS
    String ATTR_ID = "subject_id";
    String ATTR_MEMBER_OF = "member_of";
    // COMMON
    //String SUBJECT_NAME = ObjectClassUtil.createSpecialName("subject");
    String SUBJECT_NAME = ObjectProcessing.SUBJECT_NAME;
    String ATTR_ID_IDX = "id_index";
    String ATTR_MODIFIED = "last_modified";
    String ATTR_DELETED = "deleted";
}
