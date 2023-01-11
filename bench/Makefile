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
build: test

.PHONY: test
test:
	go test github.com/atomix/atomix/bench/...

.PHONY: docker
docker:
	docker build . -t atomix/bench:latest -f build/Dockerfile

.PHONY: publish
publish: docker
	docker push atomix/bench:latest
ifdef TAG
	docker tag atomix/bench:latest atomix/bench:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/bench:latest atomix/bench:${MAJOR}.${MINOR}
	docker push atomix/bench:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/bench:${MAJOR}.${MINOR}
endif

kind: docker
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/bench:latest

.PHONY: api
api:
	$(MAKE) -C api

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
