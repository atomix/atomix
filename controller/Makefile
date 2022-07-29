# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

ifdef TAG
VERSION := $(word 2, $(subst /, , $(TAG)))
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
	docker tag atomix/runtime-controller:latest atomix/runtime-controller:$(VERSION)
	docker push atomix/runtime-controller:$(VERSION)
endif

release-controller-init:
	docker push atomix/runtime-controller-init:latest
ifdef TAG
	docker tag atomix/runtime-controller-init:latest atomix/runtime-controller-init:$(VERSION)
	docker push atomix/runtime-controller-init:$(VERSION)
endif

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/runtime-controller:latest
	kind load docker-image atomix/runtime-controller-init:latest

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
