# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test github.com/atomix/atomix/protocols/rsm/pkg/... -p 1

.PHONY: api
api:
	@find ./pkg/api -name '*.pb.go' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
		--proto-path ./proto --go-path ./pkg/api --import-path github.com/atomix/atomix/protocols/rsm/pkg/api
