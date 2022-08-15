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
	docker build . -t atomix/runtime-controller:latest -f build/Dockerfile

build-controller-init:
	docker build . -t atomix/runtime-controller-init:latest -f build/init.Dockerfile

.PHONY: release
release: build release-controller release-controller-init

release-controller:
	docker push atomix/runtime-controller:latest
ifdef TAG
	docker tag atomix/runtime-controller:latest atomix/runtime-controller:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/runtime-controller:latest atomix/runtime-controller:${MAJOR}.${MINOR}
	docker push atomix/runtime-controller:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/runtime-controller:${MAJOR}.${MINOR}
endif

release-controller-init:
	docker push atomix/runtime-controller-init:latest
ifdef TAG
	docker tag atomix/runtime-controller-init:latest atomix/runtime-controller-init:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/runtime-controller-init:latest atomix/runtime-controller-init:${MAJOR}.${MINOR}
	docker push atomix/runtime-controller-init:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/runtime-controller-init:${MAJOR}.${MINOR}
endif

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/runtime-controller:latest
	kind load docker-image atomix/runtime-controller-init:latest

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
