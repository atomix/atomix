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
build: build-controller build-controller-init

build-controller:
	docker build . -t atomix/controller:latest -f build/Dockerfile-controller

build-controller-init:
	docker build . -t atomix/controller-init:latest -f build/Dockerfile-init

.PHONY: test
test:
	go test github.com/atomix/atomix/controller/pkg/... -p 1

.PHONY: release
release: build release-controller release-controller-init

release-controller:
	docker push atomix/controller:latest
ifdef TAG
	docker tag atomix/controller:latest atomix/controller:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/controller:latest atomix/controller:${MAJOR}.${MINOR}
	docker push atomix/controller:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/controller:${MAJOR}.${MINOR}
endif

release-controller-init:
	docker push atomix/controller-init:latest
ifdef TAG
	docker tag atomix/controller-init:latest atomix/controller-init:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/controller-init:latest atomix/controller-init:${MAJOR}.${MINOR}
	docker push atomix/controller-init:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/controller-init:${MAJOR}.${MINOR}
endif

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/controller:latest
	kind load docker-image atomix/controller-init:latest

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
