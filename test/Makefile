# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build: test docker

.PHONY: test
test:
	go test github.com/atomix/atomix/test/...

.PHONY: docker
docker:
	docker build . -t atomix/test:latest -f build/Dockerfile

kind: docker
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/test:latest
