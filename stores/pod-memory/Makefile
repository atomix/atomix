# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: controller

controller:
	docker build . -f build/controller/Dockerfile -t atomix/pod-memory-controller

kind: controller-kind

controller-kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/pod-memory-controller:latest

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/pod-memory/pkg/... -p 1
