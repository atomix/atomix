# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: sidecar controller

sidecar:
	docker build . -t atomix/sidecar -f build/sidecar/Dockerfile

controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-sidecar-controller ./cmd/atomix-sidecar-controller
	docker build ./build/controller -t atomix/sidecar-controller

kind: sidecar-kind controller-kind

sidecar-kind: sidecar
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sidecar

controller-kind: controller
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/sidecar-controller

.PHONY: test
test:
	go test github.com/atomix/atomix/sidecar/pkg/... -p 1
