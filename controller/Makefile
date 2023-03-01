# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: controller-init controller

controller:
	docker build . -f build/controller/Dockerfile -t atomix/controller

controller-init:
	docker build . -f build/controller-init/Dockerfile -t atomix/controller-init

kind: controller-kind controller-init-kind

controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/controller:latest

controller-init-kind: controller-init
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/controller-init:latest

.PHONY: test
test:
	go test github.com/atomix/atomix/controller/pkg/... -p 1
