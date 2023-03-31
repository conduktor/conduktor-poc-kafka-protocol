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

import io.conduktor.gateway.service.validator.PortRange;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class HostPortConfiguration {

    @NotBlank
    private String gatewayBindHost;
    @NotBlank
    private String gatewayHost;
    private String hostPrefix;
    //port_start:port_end
    @NotBlank
    @PortRange
    private String portRange;
    private int gatewayPort;


    public List<Integer> getPortInRange() {
        var ports = portRange.split(":");
        var start = Integer.parseInt(ports[0]);
        var end = Integer.parseInt(ports[1]);
        var portList = new ArrayList<Integer>();
        for (int i = start; i <= end; i++) {
            portList.add(i);
        }
        return portList;
    }


}
