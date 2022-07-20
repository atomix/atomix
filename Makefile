# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: api controller proxy sdk

.PHONY: api
api:
	$(MAKE) -C api build

.PHONY: controller
controller:
	$(MAKE) -C controller build

.PHONY: proxy
proxy:
	$(MAKE) -C proxy build

.PHONY: sdk
sdk:
	$(MAKE) -C sdk build
