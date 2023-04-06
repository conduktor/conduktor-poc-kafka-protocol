#!/bin/bash

#Copyright 2023 Conduktor, Inc
#
#Licensed under the Conduktor Community License (the "License"); you may not use
#this file except in compliance with the License.  You may obtain a copy of the
#License at
#
#https://www.conduktor.io/conduktor-community-license-agreement-v1.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#WARRANTIES OF ANY KIND, either express or implied.  See the License for the
#specific language governing permissions and limitations under the License.


# Setup
if [ -z "$CLASSPATH" ]; then
  echo "Warning : CLASSPATH environment variable is unset, interceptor plugin may not be loaded.  Set your CLASSPATH environment variable to include the location of your interceptor plugin jar files."
fi

if [ -z "$CONFIGURATION_FILE_PATH" ]; then
  export CONFIGURATION_FILE_PATH=gateway-core/config/application.yaml
fi

export CLASSPATH=${CLASSPATH}:gateway-core/target/gateway-core-0.1.0-SNAPSHOT.jar

# Run command:
java io.conduktor.gateway.Bootstrap

# Error handling:
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Gateway failed to start.  Ensure your CLASSPATH includes gateway-core-0.1.0-SNAPSHOT.jar and any interceptors defined under the pluginClass in your application.yaml" ${CLASSPATH}
fi
exit $retVal