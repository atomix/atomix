# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

SHELL := /bin/bash

.PHONY: build
build: node controller

kind: node-kind controller-kind

ifdef TAGS
BUILD_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/shared-memory-controller:/g' | cat <(echo -n "-t atomix/shared-memory-controller:") -)
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-shared-memory-controller ./cmd/atomix-shared-memory-controller
	docker build ./build/controller ${BUILD_CONTROLLER_ARGS}

KIND_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/shared-memory-controller:/g' | cat <(echo -n "atomix/shared-memory-controller:") -)
controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_CONTROLLER_ARGS}

BUILD_NODE_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/shared-memory-node:/g' | cat <(echo -n "-t atomix/shared-memory-node:") -)
node:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/node/atomix-shared-memory-node ./cmd/atomix-shared-memory-node
	docker build ./build/node ${BUILD_NODE_ARGS}

KIND_NODE_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/shared-memory-node:/g' | cat <(echo -n "atomix/shared-memory-node:") -)
node-kind: node
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_NODE_ARGS}
endif

ifndef TAGS
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-shared-memory-controller ./cmd/atomix-shared-memory-controller
	docker build ./build/controller -t atomix/shared-memory-controller

controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-controller

node:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/node/atomix-shared-memory-node ./cmd/atomix-shared-memory-node
	docker build ./build/node -t atomix/shared-memory-node

node-kind: node
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-node
endif

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/shared-memory/pkg/... -p 1
