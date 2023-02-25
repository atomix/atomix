# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

SHELL := /bin/bash

.PHONY: build
build: node controller

kind: node-kind controller-kind

ifdef TAGS
BUILD_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/raft-controller:/g' | cat <(echo -n "-t atomix/raft-controller:") -)
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-raft-controller ./cmd/atomix-raft-controller
	docker build ./build/controller ${BUILD_CONTROLLER_ARGS}

KIND_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/raft-controller:/g' | cat <(echo -n "atomix/raft-controller:") -)
controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_CONTROLLER_ARGS}

BUILD_NODE_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/raft-node:/g' | cat <(echo -n "-t atomix/raft-node:") -)
node:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/node/atomix-raft-node ./cmd/atomix-raft-node
	docker build ./build/node ${BUILD_NODE_ARGS}

KIND_NODE_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/raft-node:/g' | cat <(echo -n "atomix/raft-node:") -)
node-kind: node
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_NODE_ARGS}
endif

ifndef TAGS
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-raft-controller ./cmd/atomix-raft-controller
	docker build ./build/controller -t atomix/raft-controller

controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/raft-controller

node:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/node/atomix-raft-node ./cmd/atomix-raft-node
	docker build ./build/node -t atomix/raft-node

node-kind: node
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/raft-node
endif

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/raft/pkg/... -p 1

.PHONY: api
api:
	@find ./api -name '*.pb.go' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
		--proto-path ./api --go-path ./api --import-path github.com/atomix/atomix/stores/raft/api
