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

package io.conduktor.gateway.config.support;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.*;

@Slf4j
public class EnvironmentVariables {

    private static final Pattern ENV_VAR_PATTERN = Pattern.compile("(?i)\\$\\{([a-z0-9_]+)(\\s*\\|(.+)\\s*)?\\}");


    public static Properties resolve(Properties properties) {
        var propertiesResult = new Properties();
        properties.forEach((key, value) -> propertiesResult.put(key, resolve((String) value)));
        return propertiesResult;
    }

    public static String resolve(String input) {
        var resolved = input;
        resolved = resolve(resolved, ENV_VAR_PATTERN.matcher(input), EnvironmentVariables::resolveEnvVar);
        return resolved;
    }

    private static String resolve(String input, Matcher matcher, Function<String, String> resolver) {

        var sb = new StringBuilder();

        var lastIndex = 0;
        while (matcher.find()) {
            var start = matcher.start();
            var end = matcher.end();

            var envName = matcher.group(1);
            var envValue = resolver.apply(envName);
            if (StringUtils.isEmpty(envValue)) {
                if (Objects.nonNull(matcher.group(2))) {
                    envValue = matcher.group(3);
                } else {
                    log.warn("Environment variable `{}` cannot be resolved", envName);
                }
            }

            if (start > lastIndex)
                sb.append(input, lastIndex, start);

            sb.append(envValue);

            lastIndex = end;
        }

        if (lastIndex < input.length())
            sb.append(input.substring(lastIndex));

        return sb.toString().trim();
    }

    private static String resolveEnvVar(@NonNull String envName) {
        return Optional.ofNullable(getenv(envName)).orElseGet(() -> getProperty(envName, null));
    }


}
