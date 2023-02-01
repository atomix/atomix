# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: test docker

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/shared-memory/pkg/... -p 1

.PHONY: docker
docker: docker-controller docker-node

docker-controller:
	docker build . -t atomix/shared-memory-controller:latest -f build/controller/Dockerfile

docker-node:
	docker build . -t atomix/shared-memory-node:latest -f build/node/Dockerfile

kind: kind-node kind-controller
kind-node: docker-node
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-node:latest

kind-controller: docker-controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-controller:latest
