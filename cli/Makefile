# SPDX-FileCopyrightText: 2023-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	docker build . -f build/Dockerfile -t atomix/build

.PHONY: test
test:
	go test ./...
