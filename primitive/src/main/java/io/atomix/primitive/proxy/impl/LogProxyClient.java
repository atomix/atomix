// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.proxy.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.log.LogClient;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Log proxy client.
 */
public class LogProxyClient<S> extends AbstractProxyClient<S> {
  private final LogClient client;
  private final Serializer serializer;

  public LogProxyClient(
      String name,
      PrimitiveType type,
      PrimitiveProtocol protocol,
      Class<S> serviceType,
      ServiceConfig serviceConfig,
      LogClient client) {
    super(name, type, protocol, createSessions(name, type, serviceType, serviceConfig, client.getPartitions()));
    this.client = client;
    this.serializer = Serializer.using(type.namespace());
  }

  private static <S> Collection<ProxySession<S>> createSessions(
      String name,
      PrimitiveType primitiveType,
      Class<S> serviceType,
      ServiceConfig serviceConfig,
      Collection<LogSession> partitions) {
    Serializer serializer = Serializer.using(primitiveType.namespace());
    return partitions.stream()
        .map(partition -> new LogProxySession<S>(name, primitiveType, serviceType, serviceConfig, serializer, partition))
        .collect(Collectors.toList());
  }

  @Override
  public PartitionId getPartitionId(String key) {
    return client.getPartitionId(key);
  }

  @Override
  public PartitionId getPartitionId(Object key) {
    return client.getPartitionId(BaseEncoding.base16().encode(serializer.encode(key)));
  }
}
