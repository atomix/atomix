# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: test docker

.PHONY: test
test:
	go test github.com/atomix/atomix/build/... -p 1

.PHONY: docker
docker:
	docker build . -t atomix/build:latest -f build/Dockerfile
