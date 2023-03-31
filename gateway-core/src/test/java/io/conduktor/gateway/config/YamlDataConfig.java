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

package io.conduktor.gateway.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

public class YamlDataConfig {

    public String global;
    public Company company;
    public Endpoint endpoint;

    public static class Person {

        public String name;
        public int age;

    }

    public static class Company {

        public String name;
        public Address address;
        public List<Person> staff;

    }

    public static class Address {

        public String street;
        public int phone = 1000;

    }

    @Getter
    @ToString
    @NoArgsConstructor
    public static class Endpoint {

        private String host;
        private int port = 80;
    }

    // used to test we can override to null config
    public String empty = "nonEmpty";


}
