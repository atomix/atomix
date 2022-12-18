# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

build: generate

generate:
	@find . -name '*.md' -delete
	docker run -i \
		-v `pwd`:/build \
		atomix/codegen:docs-latest \
		--proto-path . --docs-path . --docs-format markdown
