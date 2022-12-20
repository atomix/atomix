# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	$(MAKE) -C multi-raft build
	$(MAKE) -C pod-memory build
	$(MAKE) -C shared-memory build

.PHONY: test
test:
	$(MAKE) -C multi-raft test
	$(MAKE) -C pod-memory test
	$(MAKE) -C shared-memory test

.PHONY: kind
kind:
	$(MAKE) -C multi-raft kind
	$(MAKE) -C pod-memory kind
	$(MAKE) -C shared-memory kind
