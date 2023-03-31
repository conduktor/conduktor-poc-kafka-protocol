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

package io.conduktor.gateway.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Paths;

public class JacksonUtils {

    {
        MAPPER.findAndRegisterModules();
    }
    private static final ObjectMapper MAPPER = new ObjectMapper();


    public static <T> T readTo(String jsonText, Class<T> tClass) throws JsonProcessingException {
        return MAPPER.readValue(jsonText, tClass);
    }

    public static <T> T readTo(String jsonText, TypeReference typeReference) throws JsonProcessingException {
        return (T)MAPPER.readValue(jsonText, typeReference);
    }

    public static <T> T fileTo(String location, TypeReference typeReference) throws IOException {
        return (T)MAPPER.readValue(Paths.get(location).toFile(),typeReference);
    }

    public static<T> String writeToString(T element) throws JsonProcessingException {
        return MAPPER.writeValueAsString(element);
    }

    public static String nodeValueAsString(JsonNode node) throws JsonProcessingException {
        return MAPPER.writeValueAsString(node);
    }


    public static ObjectMapper mapper() {
        return MAPPER;
    }

}
