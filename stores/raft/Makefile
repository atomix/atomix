# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: test docker

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

.PHONY: docker
docker: docker-controller docker-node

docker-controller:
	docker build . -t atomix/raft-controller:latest -f build/controller/Dockerfile

docker-node:
	docker build . -t atomix/raft-node:latest -f build/node/Dockerfile

kind: docker
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/raft-controller:latest
	kind load docker-image atomix/raft-node:latest
