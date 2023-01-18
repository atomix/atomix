# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: go docs
	go build ./...

.PHONY: test
test:
	go test github.com/atomix/atomix/api/...

go:
	@find ./runtime -name '*.pb.go' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
		--proto-path . --go-path . --import-path github.com/atomix/atomix/api

docs:
	@find ./runtime -name '*.md' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:docs-latest \
		--proto-path . --docs-path . --docs-format markdown
