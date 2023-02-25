# SPDX-FileCopyrightText: 2023-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

SHELL := /bin/bash

.PHONY: build

ifdef TAGS
BUILD_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/cli:/g' | cat <(echo -n "-t atomix/cli:") -)
build:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/atomix ./cmd/atomix
	docker build ./build ${BUILD_ARGS}

KIND_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/cli:/g' | cat <(echo -n "atomix/cli:") -)
kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_ARGS}
endif

ifndef TAGS
build:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/atomix ./cmd/atomix
	docker build ./build -t atomix/cli

kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/cli
endif

.PHONY: test
test:
	go test ./...
