# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: node controller

node:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/node/atomix-shared-memory-node ./cmd/atomix-shared-memory-node
	docker build ./build/node -t atomix/shared-memory-node

controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-shared-memory-controller ./cmd/atomix-shared-memory-controller
	docker build ./build/controller -t atomix/shared-memory-controller

kind: node controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-node
	kind load docker-image atomix/shared-memory-controller

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/shared-memory/pkg/... -p 1
