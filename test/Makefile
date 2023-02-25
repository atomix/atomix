# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/atomix-test ./cmd/atomix-test

.PHONY: test
test:
	go test github.com/atomix/atomix/test/...
