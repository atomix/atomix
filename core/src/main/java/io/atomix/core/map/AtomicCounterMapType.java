// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.core.map.impl.DefaultAtomicCounterMapBuilder;
import io.atomix.core.map.impl.DefaultAtomicCounterMapService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic counter map primitive type.
 */
public class AtomicCounterMapType<K> implements PrimitiveType<AtomicCounterMapBuilder<K>, AtomicCounterMapConfig, AtomicCounterMap<K>> {
  private static final String NAME = "atomic-counter-map";
  private static final AtomicCounterMapType INSTANCE = new AtomicCounterMapType();

  /**
   * Returns a new atomic counter map type.
   *
   * @param <K> the key type
   * @return a new atomic counter map type
   */
  @SuppressWarnings("unchecked")
  public static <K> AtomicCounterMapType<K> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultAtomicCounterMapService();
  }

  @Override
  public AtomicCounterMapConfig newConfig() {
    return new AtomicCounterMapConfig();
  }

  @Override
  public AtomicCounterMapBuilder<K> newBuilder(String name, AtomicCounterMapConfig config, PrimitiveManagementService managementService) {
    return new DefaultAtomicCounterMapBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
