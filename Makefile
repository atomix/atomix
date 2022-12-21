# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	$(MAKE) -C charts build
	$(MAKE) -C controller build
	$(MAKE) -C drivers build
	$(MAKE) -C protocols build
	$(MAKE) -C proxy build
	$(MAKE) -C runtime build
	$(MAKE) -C stores build

.PHONY: test
test:
	$(MAKE) -C api test
	$(MAKE) -C charts test
	$(MAKE) -C controller test
	$(MAKE) -C drivers test
	$(MAKE) -C protocols test
	$(MAKE) -C proxy test
	$(MAKE) -C runtime test
	$(MAKE) -C stores test

.PHONY: kind
kind:
	$(MAKE) -C controller kind
	$(MAKE) -C proxy kind
	$(MAKE) -C stores kind
