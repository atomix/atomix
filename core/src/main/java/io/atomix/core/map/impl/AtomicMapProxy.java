// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.ProxyClient;

import java.time.Duration;
import java.util.Collection;

/**
 * Distributed resource providing the {@link AsyncAtomicMap} primitive.
 */
public class AtomicMapProxy extends AbstractAtomicMapProxy<AsyncAtomicMap<String, byte[]>, AtomicMapService<String>, String>
    implements AsyncAtomicMap<String, byte[]>, AtomicMapClient<String> {
  public AtomicMapProxy(ProxyClient<AtomicMapService<String>> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  protected PartitionId getPartition(String key) {
    return getProxyClient().getPartitionId(key);
  }

  @Override
  protected Collection<PartitionId> getPartitions() {
    return getProxyClient().getPartitionIds();
  }

  @Override
  public AtomicMap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicMap<>(this, operationTimeout.toMillis());
  }
}
