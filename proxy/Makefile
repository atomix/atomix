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
build:
	docker build . -t atomix/runtime-proxy:latest

.PHONY: release
release: build
	docker push atomix/runtime-proxy:latest
ifdef TAG
	docker tag atomix/runtime-proxy:latest atomix/runtime-proxy:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/runtime-proxy:latest atomix/runtime-proxy:${MAJOR}.${MINOR}
	docker push atomix/runtime-proxy:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/runtime-proxy:${MAJOR}.${MINOR}
endif

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/runtime-proxy:latest

.PHONY: api
api:
	$(MAKE) -C api

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
