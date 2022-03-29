// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.core.map.impl.DefaultAtomicNavigableMapService;
import io.atomix.core.map.impl.DefaultAtomicSortedMapBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Consistent sorted map primitive type.
 */
public class AtomicSortedMapType<K extends Comparable<K>, V>
    implements PrimitiveType<AtomicSortedMapBuilder<K, V>, AtomicSortedMapConfig, AtomicSortedMap<K, V>> {
  private static final String NAME = "atomic-sorted-map";
  private static final AtomicSortedMapType INSTANCE = new AtomicSortedMapType();

  /**
   * Returns a new consistent tree map type.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new consistent tree map type
   */
  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>, V> AtomicSortedMapType<K, V> instance() {
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
    return new DefaultAtomicNavigableMapService<>();
  }

  @Override
  public AtomicSortedMapConfig newConfig() {
    return new AtomicSortedMapConfig();
  }

  @Override
  public AtomicSortedMapBuilder<K, V> newBuilder(String name, AtomicSortedMapConfig config, PrimitiveManagementService managementService) {
    return new DefaultAtomicSortedMapBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
