# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.DEFAULT_GOAL := help

.PHONY: help
help:	## Show the help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build atomix
	$(MAKE) -C api build
	$(MAKE) -C controller build
	$(MAKE) -C drivers build
	$(MAKE) -C logging build
	$(MAKE) -C protocols build
	$(MAKE) -C runtime build
	$(MAKE) -C sidecar build
	$(MAKE) -C stores build
	$(MAKE) -C testing build

.PHONY: test
test: ## Test atomix
	$(MAKE) -C api test
	$(MAKE) -C controller test
	$(MAKE) -C drivers test
	$(MAKE) -C logging test
	$(MAKE) -C protocols test
	$(MAKE) -C runtime test
	$(MAKE) -C sidecar test
	$(MAKE) -C stores test
	$(MAKE) -C testing test

.PHONY: kind
kind: ## Load controller, sidecar, and stores into Kind cluster
	$(MAKE) -C controller kind
	$(MAKE) -C sidecar kind
	$(MAKE) -C stores kind
