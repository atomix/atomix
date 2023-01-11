# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

ifdef TAG
VERSION := $(word 2, $(subst /, , $(TAG)))
MAJOR := $(word 1, $(subst ., , $(VERSION)))
MINOR := $(word 2, $(subst ., , $(VERSION)))
PATCH := $(word 3, $(subst ., , $(VERSION)))
endif

.PHONY: docker
docker: docker-controller docker-node

docker-controller:
	docker build . -t atomix/shared-memory-controller:latest -f build/controller/Dockerfile

docker-node:
	docker build . -t atomix/shared-memory-node:latest -f build/node/Dockerfile

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/shared-memory/pkg/... -p 1

.PHONY: publish
publish: publish-controller publish-node

publish-controller: docker-controller
	docker push atomix/shared-memory-controller:latest
ifdef TAG
	docker tag atomix/shared-memory-controller:latest atomix/shared-memory-controller:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/shared-memory-controller:latest atomix/shared-memory-controller:${MAJOR}.${MINOR}
	docker push atomix/shared-memory-controller:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/shared-memory-controller:${MAJOR}.${MINOR}
endif

publish-node: docker-node
	docker push atomix/shared-memory-node:latest
ifdef TAG
	docker tag atomix/shared-memory-node:latest atomix/shared-memory-node:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/shared-memory-node:latest atomix/shared-memory-node:${MAJOR}.${MINOR}
	docker push atomix/shared-memory-node:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/shared-memory-node:${MAJOR}.${MINOR}
endif

kind: docker
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-controller:latest
	kind load docker-image atomix/shared-memory-node:latest

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
