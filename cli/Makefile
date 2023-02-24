# SPDX-FileCopyrightText: 2023-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: test docker

.PHONY: test
test:
	go test ./...

.PHONY: docker
docker:
	docker build . -t atomix/cli:latest -f build/Dockerfile
