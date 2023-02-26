# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: controller node

controller:
	docker build . -f build/controller/Dockerfile atomix/raft-controller

node:
	docker build . -f build/node/Dockerfile atomix/raft-node

kind: controller-kind node-kind

controller-kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/raft-controller:latest

node-kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/raft-node:latest

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
