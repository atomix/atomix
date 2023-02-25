# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

SHELL := /bin/bash

.PHONY: build
build: sidecar controller

kind: sidecar-kind controller-kind

ifdef TAGS
BUILD_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/sidecar-controller:/g' | cat <(echo -n "-t atomix/sidecar-controller:") -)
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-sidecar-controller ./cmd/atomix-sidecar-controller
	docker build ./build/controller ${BUILD_CONTROLLER_ARGS}

KIND_CONTROLLER_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/sidecar-controller:/g' | cat <(echo -n "atomix/sidecar-controller:") -)
controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_CONTROLLER_ARGS}

BUILD_SIDECAR_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/sidecar:/g' | cat <(echo -n "-t atomix/sidecar:") -)
sidecar:
	docker build . -f ./build/sidecar/Dockerfile ${BUILD_SIDECAR_ARGS}

KIND_SIDECAR_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/sidecar:/g' | cat <(echo -n "atomix/sidecar:") -)
sidecar-kind: sidecar
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_SIDECAR_ARGS}
endif

ifndef TAGS
controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-sidecar-controller ./cmd/atomix-sidecar-controller
	docker build ./build/controller -t atomix/controller

controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/controller

sidecar:
	docker build . -f ./build/sidecar/Dockerfile -t atomix/sidecar

sidecar-kind: sidecar
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sidecar
endif

.PHONY: test
test:
	go test github.com/atomix/atomix/sidecar/pkg/... -p 1
