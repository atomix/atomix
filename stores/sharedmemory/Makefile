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
	docker build . -t atomix/sharedmemory-controller:latest -f build/controller.Dockerfile

build-node:
	docker build . -t atomix/sharedmemory-node:latest -f build/node.Dockerfile

.PHONY: release
release: release-controller release-node

release-controller: build
	docker push atomix/sharedmemory-controller:latest
ifdef TAG
	docker tag atomix/sharedmemory-controller:latest atomix/sharedmemory-controller:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/sharedmemory-controller:latest atomix/sharedmemory-controller:${MAJOR}.${MINOR}
	docker push atomix/sharedmemory-controller:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/sharedmemory-controller:${MAJOR}.${MINOR}
endif

release-node: build
	docker push atomix/sharedmemory-node:latest
ifdef TAG
	docker tag atomix/sharedmemory-node:latest atomix/sharedmemory-node:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/sharedmemory-node:latest atomix/sharedmemory-node:${MAJOR}.${MINOR}
	docker push atomix/sharedmemory-node:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/sharedmemory-node:${MAJOR}.${MINOR}
endif

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sharedmemory-controller:latest
	kind load docker-image atomix/sharedmemory-node:latest

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
