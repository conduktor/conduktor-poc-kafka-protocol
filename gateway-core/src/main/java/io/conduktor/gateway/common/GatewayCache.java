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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface GatewayCache<K,V> {


    V get(Object o);

    V put(K key, V value);

    V remove(Object o);

    void putAll(Map<? extends K, ? extends V> map);

    void clear();

    int size();

    boolean isEmpty();

    boolean containsKey(Object o);

    boolean containsValue(Object o);

    Set<K> keySet();

    V getOrDefault(Object key, V defaultValue);

    void close() throws IOException;

    Map<K, V> getAll();
}
