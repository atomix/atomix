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
	docker build . -t atomix/multiraft-controller:latest -f build/Dockerfile

.PHONY: api
api:
	@find ./pkg/api -name '*.pb.go' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
		--proto-path ./proto --go-path ./pkg/api --import-path github.com/atomix/atomix/stores/multiraft/pkg/api

.PHONY: release
release: build
	docker push atomix/multiraft-controller:latest
ifdef TAG
	docker tag atomix/multiraft-controller:latest atomix/multiraft-controller:${MAJOR}.${MINOR}.${PATCH}
	docker tag atomix/multiraft-controller:latest atomix/multiraft-controller:${MAJOR}.${MINOR}
	docker push atomix/multiraft-controller:${MAJOR}.${MINOR}.${PATCH}
	docker push atomix/multiraft-controller:${MAJOR}.${MINOR}
endif

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/multiraft-controller:latest

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
