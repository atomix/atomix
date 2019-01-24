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

import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.map.impl.DefaultAtomicMapBuilder;
import io.atomix.core.map.impl.DefaultAtomicMapService;
import io.atomix.core.map.impl.MapEntryUpdateResult;
import io.atomix.core.map.impl.MapUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.impl.CommitResult;
import io.atomix.core.transaction.impl.PrepareResult;
import io.atomix.core.transaction.impl.RollbackResult;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.time.Versioned;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Consistent map primitive type.
 */
public class AtomicMapType<K, V> implements PrimitiveType<AtomicMapBuilder<K, V>, AtomicMapConfig, AtomicMap<K, V>> {
  private static final String NAME = "atomic-map";

  private static final AtomicMapType INSTANCE = new AtomicMapType();

  /**
   * Returns a new consistent map type.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new consistent map type
   */
  @SuppressWarnings("unchecked")
  public static <K, V> AtomicMapType<K, V> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return Namespace.builder()
        .register(PrimitiveType.super.namespace())
        .register(Namespaces.BASIC)
        .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
        .register(TransactionId.class)
        .register(TransactionLog.class)
        .register(MapUpdate.class)
        .register(MapUpdate.Type.class)
        .register(PrepareResult.class)
        .register(CommitResult.class)
        .register(RollbackResult.class)
        .register(MapEntryUpdateResult.class)
        .register(MapEntryUpdateResult.Status.class)
        .register(Versioned.class)
        .register(AtomicMapEvent.class)
        .register(AtomicMapEvent.Type.class)
        .register(IteratorBatch.class)
        .register(Versioned.class)
        .register(byte[].class)
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultAtomicMapService();
  }

  @Override
  public AtomicMapConfig newConfig() {
    return new AtomicMapConfig();
  }

  @Override
  public AtomicMapBuilder<K, V> newBuilder(String name, AtomicMapConfig config, PrimitiveManagementService managementService) {
    return new DefaultAtomicMapBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
