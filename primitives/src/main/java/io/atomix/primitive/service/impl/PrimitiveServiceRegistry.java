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
package io.atomix.primitive.service.impl;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * State machine registry.
 */
public class PrimitiveServiceRegistry {
  private final Map<String, Supplier<PrimitiveService>> services = new ConcurrentHashMap<>();

  /**
   * Returns the number of registered state machines.
   *
   * @return The number of registered state machines.
   */
  public int size() {
    return services.size();
  }

  /**
   * Registers a new state machine type.
   *
   * @param type    The state machine type to register.
   * @param factory The state machine factory.
   * @return The state machine registry.
   */
  public PrimitiveServiceRegistry register(PrimitiveType type, Supplier<PrimitiveService> factory) {
    return register(type.id(), factory);
  }

  /**
   * Registers a new state machine type.
   *
   * @param type    The state machine type to register.
   * @param factory The state machine factory.
   * @return The state machine registry.
   */
  public PrimitiveServiceRegistry register(String type, Supplier<PrimitiveService> factory) {
    services.put(checkNotNull(type, "type cannot be null"), checkNotNull(factory, "factory cannot be null"));
    return this;
  }

  /**
   * Unregisters the given state machine type.
   *
   * @param type The state machine type to unregister.
   * @return The state machine registry.
   */
  public PrimitiveServiceRegistry unregister(PrimitiveType type) {
    services.remove(type.id());
    return this;
  }

  /**
   * Returns the factory for the given state machine type.
   *
   * @param type The state machine type for which to return the factory.
   * @return The factory for the given state machine type or {@code null} if the type is not registered.
   */
  public Supplier<PrimitiveService> getFactory(String type) {
    return services.get(type);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("stateMachines", services)
        .toString();
  }

}
