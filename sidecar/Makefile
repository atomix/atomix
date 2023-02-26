# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: sidecar controller

controller:
	docker build . -f build/controller/Dockerfile atomix/sidecar-controller

sidecar:
	docker build . -f build/sidecar/Dockerfile atomix/sidecar

kind: controller-kind sidecar-kind

controller-kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sidecar-controller:latest

sidecar-kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sidecar:latest

.PHONY: test
test:
	go test github.com/atomix/atomix/sidecar/pkg/... -p 1
