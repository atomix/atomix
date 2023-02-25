# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: build-controller-init build-controller

build-controller-init:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller-init/atomix-controller-init ./cmd/atomix-controller-init
	docker build ./build/controller-init -t atomix/controller-init

build-controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-controller ./cmd/atomix-controller
	docker build ./build/controller -t atomix/controller

kind: kind-controller-init kind-controller

kind-controller-init: build-controller-init
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/controller-init

kind-controller: build-controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/controller

.PHONY: test
test:
	go test github.com/atomix/atomix/controller/pkg/... -p 1
