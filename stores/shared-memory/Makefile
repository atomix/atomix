# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: controller node

controller:
	docker build . -f build/controller/Dockerfile -t atomix/shared-memory-controller

node:
	docker build . -f build/node/Dockerfile -t atomix/shared-memory-node

kind: controller-kind node-kind

controller-kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-controller:latest

node-kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-node:latest

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/shared-memory/pkg/... -p 1
