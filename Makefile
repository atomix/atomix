# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: build-controller build-proxy

.PHONY: build-controller
build-controller:
	$(MAKE) -C controller build

.PHONY: build-proxy
build-proxy:
	$(MAKE) -C proxy build
