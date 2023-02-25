# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

SHELL := /bin/bash

.PHONY: build
build: controller

kind: controller-kind

ifdef TAGS
BUILD_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/pod-memory-controller:/g' | cat <(echo -n "-t atomix/pod-memory-controller:") -)
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-pod-memory-controller ./cmd/atomix-pod-memory-controller
	docker build ./build/controller ${BUILD_CONTROLLER_ARGS}

KIND_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/pod-memory-controller:/g' | cat <(echo -n "atomix/pod-memory-controller:") -)
controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_CONTROLLER_ARGS}
endif

ifndef TAGS
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-pod-memory-controller ./cmd/atomix-pod-memory-controller
	docker build ./build/controller -t atomix/pod-memory-controller

controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/pod-memory-controller

endif

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/pod-memory/pkg/... -p 1
