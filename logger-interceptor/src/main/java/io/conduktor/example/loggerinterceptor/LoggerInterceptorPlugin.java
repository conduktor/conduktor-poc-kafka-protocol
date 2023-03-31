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

package io.conduktor.example.loggerinterceptor;


import io.conduktor.gateway.interceptor.Interceptor;
import io.conduktor.gateway.interceptor.Plugin;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class LoggerInterceptorPlugin implements Plugin {

    private String prefix;
    @Override
    public void configure(Map<String, Object> config) {

        var loggingStyle = config.get("loggingStyle");
        if (loggingStyle.equals("obiWan")) {
            this.prefix = "Hello there";
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<Interceptor> getInterceptors() {
        return List.of(new AllLoggerInterceptor(prefix),
                new FetchRequestLoggerInterceptor(),
                new FetchResponseLoggerInterceptor(),
                new ProduceLoggerInterceptor(),
                new ResponseLoggerInterceptor());
    }
}
