# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

SHELL := /bin/bash

.PHONY: build

ifdef TAGS
BUILD_ARGS := $(shell echo "${TAGS}" | sed 's/,/ -t atomix\/test:/g' | cat <(echo -n "-t atomix/test:") -)
build:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/atomix ./cmd/atomix
	docker build ./build ${BUILD_ARGS}

KIND_ARGS := $(shell echo "${TAGS}" | sed 's/,/ atomix\/test:/g' | cat <(echo -n "atomix/test:") -)
kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image ${KIND_ARGS}
endif

ifndef TAGS
build:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/atomix-test ./cmd/atomix-test
	docker build ./build -t atomix/test

kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/test
endif

.PHONY: test
test:
	go test github.com/atomix/atomix/test/...
