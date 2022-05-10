#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

set -o errexit
set -o nounset
set -o pipefail

CODEGEN_ROOT=$1
SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/../..

bash "${CODEGEN_ROOT}"/generate-groups.sh "deepcopy" \
  ./pkg/apis ./pkg/apis \
  atomix:v1beta1 \
  --go-header-file "${SCRIPT_ROOT}"/build/codegen/boilerplate.go.txt
