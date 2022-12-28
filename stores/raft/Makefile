# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

ifdef TAG
VERSION := $(word 2, $(subst /, , $(TAG)))
MAJOR := $(word 1, $(subst ., , $(VERSION)))
MINOR := $(word 2, $(subst ., , $(VERSION)))
PATCH := $(word 3, $(subst ., , $(VERSION)))
endif

.PHONY: build
build: build-controller build-node

build-controller:
	docker build . -t atomix/raft-controller:latest -f build/Dockerfile-controller

build-node:
	docker build . -t atomix/raft-node:latest -f build/Dockerfile-node

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/raft/pkg/... -p 1

.PHONY: release
release: release-controller release-node

release-controller: build
	docker push atomix/raft-controller:latest
ifdef TAG
	docker tag atomix/raft-controller:latest atomix/raft-controller:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/raft-controller:latest atomix/raft-controller:${MAJOR}.${MINOR}
	docker push atomix/raft-controller:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/raft-controller:${MAJOR}.${MINOR}
endif

release-node: build
	docker push atomix/raft-node:latest
ifdef TAG
	docker tag atomix/raft-node:latest atomix/raft-node:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/raft-node:latest atomix/raft-node:${MAJOR}.${MINOR}
	docker push atomix/raft-node:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/raft-node:${MAJOR}.${MINOR}
endif

.PHONY: api
api:
	@find ./api -name '*.pb.go' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
		--proto-path ./api --go-path ./api --import-path github.com/atomix/atomix/stores/raft/api

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/raft-controller:latest
	kind load docker-image atomix/raft-node:latest

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
