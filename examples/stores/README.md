<!--
SPDX-FileCopyrightText: 2023-present Intel Corporation
SPDX-License-Identifier: Apache-2.0
-->

# Data Store Examples

This folder contains example Kubernetes manifests for vaious data stores supported by Atomix.

## Setup

To connect to one of the data stores, you must first install Atomix in your Kubernetes cluster:

```bash
> helm install -n kube-system atomix atomix --repo https://atomix.github.io/atomix
```

## Testing

You can run the `atomix-bench` tool to connect to example stores and run benchmarks:

```bash
> helm install atomix-bench atomix-bench \
  --repo https://atomix.github.io/atomix \
  --set primitive=Set \
  --set store.name=raft-store \
  --set concurrency=1000 \
  --set writePercentage=.75
```

Follow the bench pod's logs for periodic statistics:

```bash
> kubectl logs atomix-bench-765d8d64fb-jrzkj atomix-bench -f
2023-01-14T02:07:09.294Z	INFO	main	atomix-bench/main.go:214	Starting benchmark...
2023-01-14T02:07:09.294Z	INFO	main	atomix-bench/main.go:215	concurrency: 100
2023-01-14T02:07:09.294Z	INFO	main	atomix-bench/main.go:216	sampleInterval: 10s
2023-01-14T02:07:09.294Z	INFO	main	atomix-bench/main.go:217	writePercentage: 0.500000
2023-01-14T02:07:19.275Z	INFO	main	atomix-bench/main.go:248	Completed 254056 operations in 10s (~3.934371ms/request)
2023-01-14T02:07:29.275Z	INFO	main	atomix-bench/main.go:248	Completed 189060 operations in 10s (~5.290325ms/request)
2023-01-14T02:07:39.276Z	INFO	main	atomix-bench/main.go:248	Completed 144320 operations in 10s (~6.929181ms/request)
```

## Raft

The Raft store creates a `RaftCluster` and a `RaftStore` to connect to:

```bash
> kubectl create -f examples/stores/raft-store.yaml
```

## Etcd

The etcd example can be run by first installing the `bitnami/etcd` chart:

```bash
helm install etcd bitnami/etcd --set auth.rbac.enabled=false --set auth.rbac.create=false
```

Once you've installed the chart, the example manifest creates a `DataStore` with the configuration
for connecting to the etcd cluster:

```bash
kubectl create -f examples/stores/etcd-store.yaml
```

## Redis

The etcd example can be run by first installing the `bitnami/redis` chart:

```bash
helm install redis bitnami/redis --set architecture=standalone --set auth.enabled=false
```

Once you've installed the chart, the example manifest creates a `DataStore` with the configuration
for connecting to the Redis cluster:

```bash
kubectl create -f examples/stores/redis-store.yaml
```
