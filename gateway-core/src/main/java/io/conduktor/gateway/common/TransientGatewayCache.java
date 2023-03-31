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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TransientGatewayCache<K, V> implements GatewayCache<K,V> {

    private final Map<K,V> backingMap = new ConcurrentHashMap<>();

    @Override
    public int size() {
        return backingMap.size();
    }

    @Override
    public boolean isEmpty() {
        return backingMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        return backingMap.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return backingMap.containsValue(o);
    }

    @Override
    public Set<K> keySet() {
        return backingMap.keySet();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return backingMap.getOrDefault(key, defaultValue);
    }

    @Override
    public void close() {
        // no op
    }

    @Override
    public Map<K, V> getAll() {
        return backingMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public V get(Object o) {
        return backingMap.get(o);
    }

    @Override
    public V put(K key, V value) {
        return backingMap.put(key, value);
    }

    @Override
    public V remove(Object o) {
        return backingMap.remove(o);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        backingMap.putAll(map);
    }


    @Override
    public void clear() {
        backingMap.clear();
    }

}
