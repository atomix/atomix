# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: sidecar controller

sidecar:
	docker build . -f build/sidecar/Dockerfile -t atomix/sidecar

controller:
	docker build . -f build/controller/Dockerfile -t atomix/sidecar-controller

kind: sidecar-kind controller-kind

sidecar-kind: sidecar
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sidecar:latest

controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sidecar-controller:latest

.PHONY: test
test:
	go test github.com/atomix/atomix/sidecar/pkg/...
	ct install --namespace kube-system --charts ./chart --helm-extra-set-args '--set image.pullPolicy=Never --set init.image.pullPolicy=Never --set test.image.pullPolicy=Never --set test.enabled=true'
