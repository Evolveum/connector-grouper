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
import org.identityconnectors.framework.common.objects.filter.*;

public class FilterHandler implements FilterVisitor<String, String> {

    private static final Log LOG = Log.getLog(FilterHandler.class);
    @Override
    public String visitAndFilter(String s, AndFilter andFilter) {
        return null;
    }

    @Override
    public String visitContainsFilter(String s, ContainsFilter containsFilter) {
        return null;
    }

    @Override
    public String visitContainsAllValuesFilter(String s, ContainsAllValuesFilter containsAllValuesFilter) {
        return null;
    }

    @Override
    public String visitEqualsFilter(String s, EqualsFilter equalsFilter) {
        return null;
    }

    @Override
    public String visitExtendedFilter(String s, Filter filter) {
        return null;
    }

    @Override
    public String visitGreaterThanFilter(String s, GreaterThanFilter greaterThanFilter) {
        return null;
    }

    @Override
    public String visitGreaterThanOrEqualFilter(String s, GreaterThanOrEqualFilter greaterThanOrEqualFilter) {
        return null;
    }

    @Override
    public String visitLessThanFilter(String s, LessThanFilter lessThanFilter) {
        return null;
    }

    @Override
    public String visitLessThanOrEqualFilter(String s, LessThanOrEqualFilter lessThanOrEqualFilter) {
        return null;
    }

    @Override
    public String visitNotFilter(String s, NotFilter notFilter) {
        return null;
    }

    @Override
    public String visitOrFilter(String s, OrFilter orFilter) {
        return null;
    }

    @Override
    public String visitStartsWithFilter(String s, StartsWithFilter startsWithFilter) {
        return null;
    }

    @Override
    public String visitEndsWithFilter(String s, EndsWithFilter endsWithFilter) {
        return null;
    }

    @Override
    public String visitEqualsIgnoreCaseFilter(String s, EqualsIgnoreCaseFilter equalsIgnoreCaseFilter) {
        return null;
    }
}
