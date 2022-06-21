# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

GOLANG_CROSS_VERSION := v1.18.2-v1.8.3

.PHONY: build
build:
	go build ./...

.PHONY: api
api:
	$(MAKE) -C api

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
