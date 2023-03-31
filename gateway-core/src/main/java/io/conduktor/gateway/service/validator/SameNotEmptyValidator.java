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

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.Collection;

@Slf4j
public class SameNotEmptyValidator implements ConstraintValidator<SameNotEmpty, Object> {
    private String[] fields;

    @Override
    public void initialize(SameNotEmpty constraintAnnotation) {
        fields = constraintAnnotation.fields();
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
        int emptyFieldCount = 0;
        for (String field : fields) {
            try {
                Object fieldValue = FieldUtils.readField(value, field, true);
                if (fieldValue == null) {
                    emptyFieldCount++;
                    continue;
                }
                if (fieldValue instanceof Collection<?> collection && CollectionUtils.isEmpty(collection)) {
                    emptyFieldCount++;
                }
            } catch (Exception e) {
                log.warn("Failed to read field value from {}", value, e);
            }
        }
        if (emptyFieldCount > 1) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate("must not be empty at a same time").addPropertyNode(String.join(" and ", fields)).addConstraintViolation();
            return false;
        }
        return true;
    }
}
