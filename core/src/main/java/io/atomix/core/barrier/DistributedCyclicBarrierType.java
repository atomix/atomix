/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.barrier;

import io.atomix.core.barrier.impl.CyclicBarrierResult;
import io.atomix.core.barrier.impl.DefaultDistributedCyclicBarrierService;
import io.atomix.core.barrier.impl.DefaultDistributedCyclicBarrierBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed cyclic barrier primitive type.
 */
public class DistributedCyclicBarrierType implements PrimitiveType<DistributedCyclicBarrierBuilder, DistributedCyclicBarrierConfig, DistributedCyclicBarrier> {
  private static final String NAME = "cyclic-barrier";
  private static final DistributedCyclicBarrierType INSTANCE = new DistributedCyclicBarrierType();

  /**
   * Returns a new distributed count down latch type.
   *
   * @return a new distributed count down latch type
   */
  public static DistributedCyclicBarrierType instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return Namespace.builder()
        .register(PrimitiveType.super.namespace())
        .register(CyclicBarrierResult.class)
        .register(CyclicBarrierResult.Status.class)
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultDistributedCyclicBarrierService();
  }

  @Override
  public DistributedCyclicBarrierConfig newConfig() {
    return new DistributedCyclicBarrierConfig();
  }

  @Override
  public DistributedCyclicBarrierBuilder newBuilder(String name, DistributedCyclicBarrierConfig config, PrimitiveManagementService managementService) {
    return new DefaultDistributedCyclicBarrierBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
