# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: node controller

controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-shared-memory-controller ./cmd/atomix-shared-memory-controller

node:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/node/atomix-shared-memory-node ./cmd/atomix-shared-memory-node

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/shared-memory/pkg/... -p 1
