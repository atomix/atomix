// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.core.map.impl.DefaultDistributedNavigableMapBuilder;
import io.atomix.core.map.impl.DefaultDistributedNavigableMapService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed tree map primitive type.
 */
public class DistributedNavigableMapType<K extends Comparable<K>, V> implements PrimitiveType<DistributedNavigableMapBuilder<K, V>, DistributedNavigableMapConfig, DistributedNavigableMap<K, V>> {
  private static final String NAME = "navigable-map";

  private static final DistributedNavigableMapType INSTANCE = new DistributedNavigableMapType();

  /**
   * Returns a new distributed tree map type.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new distributed tree map type
   */
  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>, V> DistributedNavigableMapType<K, V> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return AtomicNavigableMapType.instance().namespace();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultDistributedNavigableMapService();
  }

  @Override
  public DistributedNavigableMapConfig newConfig() {
    return new DistributedNavigableMapConfig();
  }

  @Override
  public DistributedNavigableMapBuilder<K, V> newBuilder(String name, DistributedNavigableMapConfig config, PrimitiveManagementService managementService) {
    return new DefaultDistributedNavigableMapBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
