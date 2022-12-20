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
	docker build . -t atomix/multi-raft-controller:latest -f build/controller.Dockerfile

build-node:
	docker build . -t atomix/multi-raft-node:latest -f build/node.Dockerfile

.PHONY: release
release: release-controller release-node

release-controller: build
	docker push atomix/multi-raft-controller:latest
ifdef TAG
	docker tag atomix/multi-raft-controller:latest atomix/multi-raft-controller:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/multi-raft-controller:latest atomix/multi-raft-controller:${MAJOR}.${MINOR}
	docker push atomix/multi-raft-controller:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/multi-raft-controller:${MAJOR}.${MINOR}
endif

release-node: build
	docker push atomix/multi-raft-node:latest
ifdef TAG
	docker tag atomix/multi-raft-node:latest atomix/multi-raft-node:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/multi-raft-node:latest atomix/multi-raft-node:${MAJOR}.${MINOR}
	docker push atomix/multi-raft-node:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/multi-raft-node:${MAJOR}.${MINOR}
endif

.PHONY: api
api:
	@find ./pkg/api -name '*.pb.go' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
		--proto-path ./proto --go-path ./pkg/api --import-path github.com/atomix/atomix/stores/multi-raft/pkg/api

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/multi-raft-controller:latest
	kind load docker-image atomix/multi-raft-node:latest

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
