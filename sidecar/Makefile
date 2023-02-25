# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: controller

controller:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/controller/atomix-sidecar-controller ./cmd/atomix-sidecar-controller

.PHONY: test
test:
	go test github.com/atomix/atomix/sidecar/pkg/... -p 1
