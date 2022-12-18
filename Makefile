# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: api common controller driver proto proxy runtime

.PHONY: api
api:
	@find ./api -name '*.pb.go' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
		--proto-path ./proto --go-path ./api/pkg --import-path github.com/atomix/atomix/api/pkg

.PHONY: common
common:
	$(MAKE) -C common build

.PHONY: controller
controller:
	$(MAKE) -C controller build

.PHONY: driver
driver:
	$(MAKE) -C driver build

.PHONY: proto
proto:
	$(MAKE) -C proto build

.PHONY: proxy
proxy:
	$(MAKE) -C proxy build

.PHONY: runtime
runtime:
	$(MAKE) -C runtime build
