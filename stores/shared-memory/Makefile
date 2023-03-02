# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: controller controller-test node

controller:
	docker build . -f build/controller/Dockerfile -t atomix/shared-memory-controller

controller-test:
	docker build . -f build/controller-test/Dockerfile -t atomix/shared-memory-controller-test

node:
	docker build . -f build/node/Dockerfile -t atomix/shared-memory-node

kind: controller-kind controller-test-kind node-kind

controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-controller:latest

node-kind: node
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-node:latest

controller-test-kind: controller-test
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/shared-memory-controller-test:latest

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/shared-memory/pkg/...
	ct install --namespace kube-system --charts ./chart --helm-extra-set-args '--set image.pullPolicy=Never --set init.image.pullPolicy=Never --set test.image.pullPolicy=Never --set test.enabled=true'
