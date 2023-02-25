# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: controller

controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-pod-memory-controller ./cmd/atomix-pod-memory-controller
	docker build ./build/controller -t atomix/pod-memory-controller

kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/pod-memory-controller

.PHONY: test
test:
	go test github.com/atomix/atomix/stores/pod-memory/pkg/... -p 1
