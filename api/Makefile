# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

ROOT := $(basename $(dir $(abspath $(dir $$PWD))))

.PHONY: build
build:
	go build ./...

generate:
	@find . -name '*.pb.go' -delete
	docker run -i \
		-v $(ROOT):/build \
		atomix/codegen:go-latest \
		--proto-path ./proto --go-path ./api/pkg --import-path github.com/atomix/atomix/api/pkg
