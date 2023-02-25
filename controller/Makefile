# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

SHELL := /bin/bash

.PHONY: build
build: controller-init controller

kind: controller-init-kind controller-kind

ifdef TAGS
BUILD_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/controller:/g' | cat <(echo -n "-t atomix/controller:") -)
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-controller ./cmd/atomix-controller
	docker build ./build/controller ${BUILD_CONTROLLER_ARGS}

KIND_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/controller:/g' | cat <(echo -n "atomix/controller:") -)
controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_CONTROLLER_ARGS}

BUILD_INIT_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/controller-init:/g' | cat <(echo -n "-t atomix/controller-init:") -)
controller-init:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller-init/atomix-controller-init ./cmd/atomix-controller-init
	docker build ./build/controller-init ${BUILD_INIT_ARGS}

KIND_INIT_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/controller-init:/g' | cat <(echo -n "atomix/controller-init:") -)
controller-init-kind: controller-init
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_INIT_ARGS}
endif

ifndef TAGS
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-controller ./cmd/atomix-controller
	docker build ./build/controller -t atomix/controller

controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/controller

controller-init:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller-init/atomix-controller-init ./cmd/atomix-controller-init
	docker build ./build/controller-init -t atomix/controller-init

controller-init-kind: controller-init
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/controller-init
endif

.PHONY: test
test:
	go test github.com/atomix/atomix/controller/pkg/... -p 1
