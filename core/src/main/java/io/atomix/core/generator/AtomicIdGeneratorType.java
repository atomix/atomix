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
package io.atomix.core.generator;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.core.counter.impl.AtomicCounterService;
import io.atomix.core.generator.impl.DelegatingAtomicIdGeneratorBuilder;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic ID generator primitive type.
 */
public class AtomicIdGeneratorType implements PrimitiveType<AtomicIdGeneratorBuilder, AtomicIdGenerator> {
  private static final String NAME = "ID_GENERATOR";

  /**
   * Returns a new atomic ID generator type.
   *
   * @return a new atomic ID generator type
   */
  public static AtomicIdGeneratorType instance() {
    return new AtomicIdGeneratorType();
  }

  private AtomicIdGeneratorType() {
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService() {
    return new AtomicCounterService();
  }

  @Override
  public AtomicIdGeneratorBuilder newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return new DelegatingAtomicIdGeneratorBuilder(name, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .toString();
  }
}
