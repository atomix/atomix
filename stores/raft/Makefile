# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: node controller

node:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/node/atomix-raft-node ./cmd/atomix-raft-node
	docker build ./build/node -t atomix/raft-node

controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-raft-controller ./cmd/atomix-raft-controller
	docker build ./build/controller -t atomix/raft-controller

kind: node controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/raft-node
	kind load docker-image atomix/raft-controller

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
