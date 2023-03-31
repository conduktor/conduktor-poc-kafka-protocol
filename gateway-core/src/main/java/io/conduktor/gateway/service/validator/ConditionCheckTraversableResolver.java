/*
 * Copyright 2023 Conduktor, Inc
 *
 * Licensed under the Conduktor Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * https://www.conduktor.io/conduktor-community-license-agreement-v1.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.conduktor.gateway.service.validator;

import jakarta.el.ELProcessor;
import jakarta.validation.Path;
import jakarta.validation.Path.Node;
import jakarta.validation.TraversableResolver;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.ElementType;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

@Slf4j
public class ConditionCheckTraversableResolver implements TraversableResolver {

    @Override
    public boolean isReachable(Object traversableObject, Node traversableProperty, Class<?> rootBeanType, Path pathToTraversableObject, ElementType elementType) {
        if (elementType != ElementType.FIELD || traversableObject == null) {
            return true;
        }
        Set<String> ignoredProperties = getSkipValidateFields(traversableObject, rootBeanType);
        String property = traversableProperty.getName();
        if (ignoredProperties.contains(property)) {
            log.info("Skip check {} when match condition", property);
        }
        return !ignoredProperties.contains(property);
    }

    @Override
    public boolean isCascadable(Object traversableObject, Node traversableProperty, Class<?> rootBeanType, Path pathToTraversableObject, ElementType elementType) {
        return true;
    }

    private Set<String> getSkipValidateFields(@NotNull Object traversableObject, Class<?> rootBeanType) {
        IgnoreWhen[] ignoreWhens = traversableObject.getClass().getAnnotationsByType(IgnoreWhen.class);
        if (ignoreWhens.length == 0) {
            return Collections.emptySet();
        }
        ELProcessor elp = new ELProcessor();
        elp.defineBean("this", traversableObject);
        Set<String> ignoreFields = new TreeSet<>();
        for (IgnoreWhen ignoreWhen : ignoreWhens) {
            boolean match = elp.getValue(ignoreWhen.when(), boolean.class);
            if (match) {
                ignoreFields.addAll(Set.of(ignoreWhen.fields()));
            }
        }
        return ignoreFields;
    }
}
