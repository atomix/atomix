# SPDX-FileCopyrightText: 2023-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o build/atomix ./cmd/atomix

.PHONY: test
test:
	go test ./...
