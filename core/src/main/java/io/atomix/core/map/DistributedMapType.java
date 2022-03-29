// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.core.map.impl.DefaultDistributedMapBuilder;
import io.atomix.core.map.impl.DefaultDistributedMapService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed map primitive type.
 */
public class DistributedMapType<K, V> implements PrimitiveType<DistributedMapBuilder<K, V>, DistributedMapConfig, DistributedMap<K, V>> {
  private static final String NAME = "map";

  private static final DistributedMapType INSTANCE = new DistributedMapType();

  /**
   * Returns a new distributed map type.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new distributed map type
   */
  @SuppressWarnings("unchecked")
  public static <K, V> DistributedMapType<K, V> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return AtomicMapType.instance().namespace();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultDistributedMapService();
  }

  @Override
  public DistributedMapConfig newConfig() {
    return new DistributedMapConfig();
  }

  @Override
  public DistributedMapBuilder<K, V> newBuilder(String name, DistributedMapConfig config, PrimitiveManagementService managementService) {
    return new DefaultDistributedMapBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
