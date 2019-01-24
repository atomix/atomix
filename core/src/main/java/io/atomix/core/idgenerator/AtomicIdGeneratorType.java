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
package io.atomix.core.idgenerator;

import io.atomix.core.counter.impl.DefaultAtomicCounterService;
import io.atomix.core.idgenerator.impl.DelegatingAtomicIdGeneratorBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic ID generator primitive type.
 */
public class AtomicIdGeneratorType implements PrimitiveType<AtomicIdGeneratorBuilder, AtomicIdGeneratorConfig, AtomicIdGenerator> {
  private static final String NAME = "atomic-id-generator";
  private static final AtomicIdGeneratorType INSTANCE = new AtomicIdGeneratorType();

  /**
   * Returns a new atomic ID generator type.
   *
   * @return a new atomic ID generator type
   */
  public static AtomicIdGeneratorType instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultAtomicCounterService();
  }

  @Override
  public AtomicIdGeneratorConfig newConfig() {
    return new AtomicIdGeneratorConfig();
  }

  @Override
  public AtomicIdGeneratorBuilder newBuilder(String name, AtomicIdGeneratorConfig config, PrimitiveManagementService managementService) {
    return new DelegatingAtomicIdGeneratorBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
