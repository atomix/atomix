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
	docker build . -t atomix/shared-memory-controller:latest -f build/controller.Dockerfile

build-node:
	docker build . -t atomix/shared-memory-node:latest -f build/node.Dockerfile

.PHONY: release
release: release-controller release-node

release-controller: build
	docker push atomix/shared-memory-controller:latest
ifdef TAG
	docker tag atomix/shared-memory-controller:latest atomix/shared-memory-controller:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/shared-memory-controller:latest atomix/shared-memory-controller:${MAJOR}.${MINOR}
	docker push atomix/shared-memory-controller:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/shared-memory-controller:${MAJOR}.${MINOR}
endif

release-node: build
	docker push atomix/shared-memory-node:latest
ifdef TAG
	docker tag atomix/shared-memory-node:latest atomix/shared-memory-node:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/shared-memory-node:latest atomix/shared-memory-node:${MAJOR}.${MINOR}
	docker push atomix/shared-memory-node:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/shared-memory-node:${MAJOR}.${MINOR}
endif

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-controller:latest
	kind load docker-image atomix/shared-memory-node:latest

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
