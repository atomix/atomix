/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.impl;

import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.ConsistentMapType;
import io.atomix.core.map.impl.ConsistentMapProxy;
import io.atomix.core.map.impl.TranscodingAsyncConsistentMap;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.ManagedPrimitiveRegistry;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypes;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Core primitive registry.
 */
public class CorePrimitiveRegistry implements ManagedPrimitiveRegistry {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC);

  private final PartitionService partitionService;
  private final AtomicBoolean started = new AtomicBoolean();
  private AsyncConsistentMap<String, String> primitives;

  public CorePrimitiveRegistry(PartitionService partitionService) {
    this.partitionService = checkNotNull(partitionService);
  }

  @Override
  public CompletableFuture<PrimitiveInfo> createPrimitive(String name, PrimitiveType type) {
    PrimitiveInfo info = new PrimitiveInfo(name, type);
    CompletableFuture<PrimitiveInfo> future = new CompletableFuture<>();
    primitives.putIfAbsent(name, type.id()).whenComplete((result, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else if (result == null || result.value().equals(type.id())) {
        future.complete(info);
      } else {
        future.completeExceptionally(new PrimitiveException("A different primitive with the same name already exists"));
      }
    });
    return future;
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    try {
      return primitives.entrySet()
          .get(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
          .stream()
          .map(entry -> new PrimitiveInfo(entry.getKey(), PrimitiveTypes.getPrimitiveType(entry.getValue().value())))
          .collect(Collectors.toList());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      throw new PrimitiveException(e.getCause());
    }
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    return getPrimitives()
        .stream()
        .filter(primitive -> primitive.type().equals(primitiveType))
        .collect(Collectors.toList());
  }

  @Override
  public PrimitiveInfo getPrimitive(String name) {
    try {
      return primitives.get(name)
          .thenApply(value -> value == null ? null : value.map(type -> new PrimitiveInfo(name, PrimitiveTypes.getPrimitiveType(type))).value())
          .get(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      throw new PrimitiveException(e.getCause());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PrimitiveRegistry> start() {
    PrimitiveProtocol protocol = partitionService.getSystemPartitionGroup().newProtocol();
    PrimitiveProxy proxy = protocol.newProxy(
        "primitives",
        ConsistentMapType.instance(),
        partitionService);
    return proxy.connect()
        .thenApply(v -> {
          ConsistentMapProxy mapProxy = new ConsistentMapProxy(proxy, this);
          primitives = new TranscodingAsyncConsistentMap<>(
              mapProxy,
              key -> key,
              key -> key,
              value -> value != null ? SERIALIZER.encode(value) : null,
              value -> value != null ? SERIALIZER.decode(value) : null);
          started.set(true);
          return this;
        });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      return primitives.close()
          .exceptionally(e -> null);
    }
    return CompletableFuture.completedFuture(null);
  }
}
