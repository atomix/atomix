# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	$(MAKE) -C etcd build
	$(MAKE) -C pod-memory build
	$(MAKE) -C raft build
	$(MAKE) -C redis build
	$(MAKE) -C shared-memory build

.PHONY: test
test:
	$(MAKE) -C etcd test
	$(MAKE) -C pod-memory test
	$(MAKE) -C raft test
	$(MAKE) -C redis test
	$(MAKE) -C shared-memory test
