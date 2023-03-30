# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	$(MAKE) -C api build
	$(MAKE) -C charts build
	$(MAKE) -C controller build
	$(MAKE) -C drivers build
	$(MAKE) -C protocols build
	$(MAKE) -C runtime build
	$(MAKE) -C sidecar build
	$(MAKE) -C stores build
	$(MAKE) -C testing build

.PHONY: test
test:
	$(MAKE) -C api test
	$(MAKE) -C bench test
	$(MAKE) -C charts test
	$(MAKE) -C controller test
	$(MAKE) -C drivers test
	$(MAKE) -C protocols test
	$(MAKE) -C runtime test
	$(MAKE) -C sidecar test
	$(MAKE) -C stores test
	$(MAKE) -C testing test

.PHONY: kind
kind:
	$(MAKE) -C controller kind
	$(MAKE) -C sidecar kind
	$(MAKE) -C stores kind
