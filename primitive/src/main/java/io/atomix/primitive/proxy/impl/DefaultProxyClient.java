// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.proxy.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.primitive.session.SessionClient;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default proxy client.
 */
public class DefaultProxyClient<S> extends AbstractProxyClient<S> {
  private final Partitioner<String> partitioner;
  private final Serializer serializer;

  public DefaultProxyClient(
      String name,
      PrimitiveType type,
      PrimitiveProtocol protocol,
      Class<S> serviceType,
      Collection<SessionClient> partitions,
      Partitioner<String> partitioner) {
    super(name, type, protocol, createSessions(type, serviceType, partitions));
    this.partitioner = checkNotNull(partitioner);
    this.serializer = Serializer.using(type.namespace());
  }

  private static <S> Collection<ProxySession<S>> createSessions(
      PrimitiveType primitiveType, Class<S> serviceType, Collection<SessionClient> partitions) {
    Serializer serializer = Serializer.using(primitiveType.namespace());
    return partitions.stream()
        .map(partition -> new DefaultProxySession<>(partition, serviceType, serializer))
        .collect(Collectors.toList());
  }

  @Override
  public PartitionId getPartitionId(String key) {
    return partitioner.partition(key, getPartitionIds());
  }

  @Override
  public PartitionId getPartitionId(Object key) {
    return partitioner.partition(BaseEncoding.base16().encode(serializer.encode(key)), getPartitionIds());
  }
}
