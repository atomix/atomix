# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

build:
	go build ./...

generate: go docs

go:
	@find . -name '*.pb.go' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
	    --proto-path . --go-path . --import-path github.com/atomix/atomix/api

docs:
	@find . -name '*.md' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:docs-latest \
		--proto-path . --docs-path . --docs-format markdown
