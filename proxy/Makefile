# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

VERSION := latest

.PHONY: build
build:
	docker build . -t atomix/runtime-proxy:latest

.PHONY: release
release: build
	docker tag atomix/runtime-proxy:latest atomix/runtime-proxy:$(VERSION)
	docker push atomix/runtime-proxy:latest
	docker push atomix/runtime-proxy:$(VERSION)

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
