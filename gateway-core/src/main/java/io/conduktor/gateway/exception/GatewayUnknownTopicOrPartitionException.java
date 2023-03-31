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

package io.conduktor.gateway.exception;

import org.apache.kafka.common.protocol.Errors;

public class GatewayUnknownTopicOrPartitionException extends NonrecoverableException {

    private String topic;
    private Integer partition;

    public GatewayUnknownTopicOrPartitionException(String topic) {
        super(Errors.UNKNOWN_TOPIC_OR_PARTITION);
        this.topic = topic;
    }

    public GatewayUnknownTopicOrPartitionException(String topic, int partition) {
        super(Errors.UNKNOWN_TOPIC_OR_PARTITION);
        this.topic = topic;
        this.partition = partition;
    }


}
