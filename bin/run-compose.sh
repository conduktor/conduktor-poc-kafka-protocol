#!/bin/env bash

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

set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

COMPOSE_DIR="${SCRIPT_DIR}/compose"
INTERCEPTORS="${COMPOSE_DIR}/interceptors"
CONFIGURATION="${COMPOSE_DIR}/application.yaml"
DOCKER_COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  docker compose -f ${DOCKER_COMPOSE_FILE} down -v
}

setup_colors() {
  if [[ -z "${NO_COLOR-}" ]] && [[ "${TERM-}" != "dumb" ]]; then
    NOFORMAT='\033[0m' YELLOW='\033[1;33m'
  else
    NOFORMAT='' RED='' GREEN='' ORANGE='' BLUE='' PURPLE='' CYAN='' YELLOW=''
  fi
}

setup_colors

if [[ -z "$(ls ${INTERCEPTORS})" ]]; then
  echo -e "${YELLOW}Warning : No interceptors provided for gateway. To add interceptor push interceptor jars in ${INTERCEPTORS} folder and configure them.${NOFORMAT}"
fi

docker compose -f ${DOCKER_COMPOSE_FILE} up -d
docker compose -f ${DOCKER_COMPOSE_FILE} logs -f gateway

