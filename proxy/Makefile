# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

VERSION := latest

.PHONY: build
build:
	docker build . -t atomix/proxy:latest

.PHONY: release
release: build
	docker tag atomix/proxy:latest atomix/proxy:$(VERSION)
	docker push atomix/proxy:latest
	docker push atomix/proxy:$(VERSION)

kind: build
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/proxy:latest

.PHONY: api
api:
	$(MAKE) -C api

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
