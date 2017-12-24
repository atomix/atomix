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
package io.atomix.core.multimap;

import io.atomix.core.multimap.impl.ConsistentMultimapProxyBuilder;
import io.atomix.core.multimap.impl.ConsistentSetMultimapService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Consistent multimap primitive type.
 */
public class ConsistentMultimapType<K, V> implements PrimitiveType<ConsistentMultimapBuilder<K, V>, ConsistentMultimap<K, V>> {
  private static final String NAME = "CONSISTENT_MULTIMAP";

  /**
   * Returns a new consistent multimap type.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new consistent multimap type
   */
  public static <K, V> ConsistentMultimapType<K, V> instance() {
    return new ConsistentMultimapType<>();
  }

  private ConsistentMultimapType() {
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService() {
    return new ConsistentSetMultimapService();
  }

  @Override
  public ConsistentMultimapBuilder<K, V> newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return new ConsistentMultimapProxyBuilder<>(name, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .toString();
  }
}
