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
package io.atomix.primitives.multimap;

import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitives.multimap.impl.ConsistentSetMultimapService;
import io.atomix.primitives.multimap.impl.DiscreteConsistentMultimapBuilder;

/**
 * Consistent multimap primitive type.
 */
public class ConsistentMultimapType<K, V> implements PrimitiveType<ConsistentMultimapBuilder<K, V>, ConsistentMultimap<K, V>, AsyncConsistentMultimap<K, V>> {
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
  public ConsistentMultimapBuilder<K, V> newPrimitiveBuilder(String name, PrimitiveClient client) {
    return new DiscreteConsistentMultimapBuilder<>(name, client);
  }
}
