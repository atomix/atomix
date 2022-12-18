# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: api common controller driver proxy runtime

.PHONY: api
api:
	$(MAKE) -C api build

.PHONY: common
common:
	$(MAKE) -C common build

.PHONY: controller
controller:
	$(MAKE) -C controller build

.PHONY: driver
driver:
	$(MAKE) -C driver build

.PHONY: proxy
proxy:
	$(MAKE) -C proxy build

.PHONY: runtime
runtime:
	$(MAKE) -C runtime build
