# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: controller-init controller

controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-controller ./cmd/atomix-controller

controller-init:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller-init/atomix-controller-init ./cmd/atomix-controller-init

.PHONY: test
test:
	go test github.com/atomix/atomix/controller/pkg/... -p 1
