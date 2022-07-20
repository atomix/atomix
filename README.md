[![API](https://github.com/atomix/runtime/actions/workflows/api.yml/badge.svg)](https://github.com/atomix/runtime/actions/workflows/api.yml)
[![SDK](https://github.com/atomix/runtime/actions/workflows/sdk.yml/badge.svg)](https://github.com/atomix/runtime/actions/workflows/sdk.yml)
[![Controller](https://github.com/atomix/runtime/actions/workflows/controller.yml/badge.svg)](https://github.com/atomix/runtime/actions/workflows/controller.yml)
[![Proxy](https://github.com/atomix/runtime/actions/workflows/proxy.yml/badge.svg)](https://github.com/atomix/runtime/actions/workflows/proxy.yml)

# Atomix Runtime

Atomix is a cloud native runtime for building stateful, scalable, configurable, and reliable distributed 
applications in Kubernetes. The runtime API provides a set of high-level building blocks (referred to as distributed 
primitives) for building distributed systems. The architecture of the Atomix runtime incorporates the lessons learned 
from experience over the past decade building high-availability cloud infrastructure. The primary focus of the runtime 
is to decouple applications from specific data stores, instead providing a set of unified, polyglot interfaces 
(gRPC services) to a variety of systems and protocols, and enabling rapid experimentation and customization of 
distributed applications.

* `api` - provides the Protobuf API defining distributed primitives
* `sdk` - the core Go library for extending the Atomix runtime, including drivers and custom primitives
* `controller` - the runtime Kubernetes controller responsible for managing stores, injecting proxis, and 
  configuring primitives according to configuration defined in k8s custom resources
* `proxy` - the sidecar proxy used by applications to operate on distributed primitives
