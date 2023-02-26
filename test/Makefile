# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	docker build . -f build/Dockerfile atomix/test

kind:
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/test:latest

.PHONY: test
test:
	go test github.com/atomix/atomix/test/...
