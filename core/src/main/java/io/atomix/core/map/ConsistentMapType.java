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
package io.atomix.core.map;

import io.atomix.core.map.impl.ConsistentMapProxyBuilder;
import io.atomix.core.map.impl.ConsistentMapService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Consistent map primitive type.
 */
public class ConsistentMapType<K, V> implements PrimitiveType<ConsistentMapBuilder<K, V>, ConsistentMap<K, V>> {
  private static final String NAME = "CONSISTENT_MAP";

  /**
   * Returns a new consistent map type.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new consistent map type
   */
  public static <K, V> ConsistentMapType<K, V> instance() {
    return new ConsistentMapType<>();
  }

  private ConsistentMapType() {
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService() {
    return new ConsistentMapService();
  }

  @Override
  public ConsistentMapBuilder<K, V> newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return new ConsistentMapProxyBuilder<>(name, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .toString();
  }
}
