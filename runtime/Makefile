# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

build: generate
	go build ./...

.PHONY: test
test:
	go test github.com/atomix/atomix/runtime/pkg/... -p 1

generate:
	@find . -name '*.pb.go' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
	    --proto-path ../proto --go-path ./pkg --import-path github.com/atomix/atomix/api/pkg
