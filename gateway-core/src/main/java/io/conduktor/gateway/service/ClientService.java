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

package io.conduktor.gateway.service;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ClientService {

    private final Properties kafkaConnectionProperties;

    @Inject
    public ClientService(@Named("kafkaServerProperties") Properties kafkaConnectionProperties) {
        this.kafkaConnectionProperties = kafkaConnectionProperties;
    }

    public Admin getAdminClient() {
        return Admin.create(kafkaConnectionProperties);
    }

    public Admin getAdminClient(Properties properties) {
        return Admin.create(properties);
    }

    public Node getAvailableKafkaNode(Properties kafkaProperties) {
        return getKafkaNodes(kafkaProperties)
                .stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Cannot connect to Kafka: " + kafkaProperties.getProperty("bootstrap.servers")));
    }

    public List<Node> getKafkaNodes(){
        return getKafkaNodes(kafkaConnectionProperties);
    }

    private List<Node> getKafkaNodes(Properties kafkaProperties) {
        try (var adminClient = Admin.create(kafkaProperties)) {
            var nodes = adminClient.describeCluster()
                    .nodes()
                    .get(5, TimeUnit.SECONDS);
            if (CollectionUtils.isEmpty(nodes)) {
               throw new IllegalArgumentException("Cannot connect to Kafka: " + kafkaProperties.getProperty("bootstrap.servers"));
            }
            return new ArrayList<>(nodes);
        } catch (Throwable exception) {
            log.error("Error happen when get brokers in backend cluster {}", kafkaProperties.getProperty("bootstrap.servers"), exception);
            throw new IllegalArgumentException("Cannot connect to Kafka: " + kafkaProperties.getProperty("bootstrap.servers"));
        }
    }

}