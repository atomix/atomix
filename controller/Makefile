# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: test docker

.PHONY: test
test:
	go test github.com/atomix/atomix/controller/pkg/... -p 1

.PHONY: docker
docker: build-controller build-controller-init

build-controller:
	docker build . -t atomix/controller:latest -f build/Dockerfile-controller

build-controller-init:
	docker build . -t atomix/controller-init:latest -f build/Dockerfile-init

kind: docker
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/controller:latest
	kind load docker-image atomix/controller-init:latest
