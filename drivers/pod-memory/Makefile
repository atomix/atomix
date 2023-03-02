# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)

.PHONY: build
build:
	docker build $(ROOT_DIR) -f $(ROOT_DIR)/drivers/test/build/Dockerfile -t atomix/driver-test:pod-memory --build-arg driver=pod-memory --build-arg version=v1

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/driver-test:pod-memory

.PHONY: test
test:
	ct install --charts ./test
