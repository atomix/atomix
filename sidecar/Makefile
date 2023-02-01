# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: test docker

.PHONY: test
test:
	go test github.com/atomix/atomix/sidecar/pkg/... -p 1

.PHONY: docker
docker: build-sidecar build-controller

build-sidecar:
	docker build . -t atomix/sidecar:latest -f build/sidecar/Dockerfile

build-controller:
	docker build . -t atomix/sidecar-controller:latest -f build/controller/Dockerfile

kind: kind-sidecar kind-controller
kind-sidecar: build-sidecar
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sidecar:latest

kind-controller: build-controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sidecar-controller:latest
