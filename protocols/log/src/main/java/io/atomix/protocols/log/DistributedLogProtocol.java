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
package io.atomix.protocols.log;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.log.LogClient;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.LogProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.impl.LogProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.protocols.log.impl.DistributedLogClient;
import io.atomix.protocols.log.partition.LogPartition;

import java.util.Collection;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Distributed log protocol.
 */
public class DistributedLogProtocol implements LogProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns an instance of the log protocol with the default configuration.
   *
   * @return an instance of the log protocol with the default configuration
   */
  public static DistributedLogProtocol instance() {
    return new DistributedLogProtocol(new DistributedLogProtocolConfig());
  }

  /**
   * Returns a new log protocol builder.
   *
   * @return a new log protocol builder
   */
  public static DistributedLogProtocolBuilder builder() {
    return new DistributedLogProtocolBuilder(new DistributedLogProtocolConfig());
  }

  /**
   * Returns a new log protocol builder.
   *
   * @param group the partition group
   * @return the log protocol builder
   */
  public static DistributedLogProtocolBuilder builder(String group) {
    return new DistributedLogProtocolBuilder(new DistributedLogProtocolConfig().setGroup(group));
  }

  /**
   * Log protocol type.
   */
  public static final class Type implements PrimitiveProtocol.Type<DistributedLogProtocolConfig> {
    private static final String NAME = "multi-log";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public DistributedLogProtocolConfig newConfig() {
      return new DistributedLogProtocolConfig();
    }

    @Override
    public PrimitiveProtocol newProtocol(DistributedLogProtocolConfig config) {
      return new DistributedLogProtocol(config);
    }
  }

  private final DistributedLogProtocolConfig config;

  protected DistributedLogProtocol(DistributedLogProtocolConfig config) {
    this.config = checkNotNull(config, "config cannot be null");
  }

  @Override
  public PrimitiveProtocol.Type type() {
    return TYPE;
  }

  @Override
  public String group() {
    return config.getGroup();
  }

  @Override
  public LogClient newClient(PartitionService partitionService) {
    Collection<LogSession> partitions = partitionService.getPartitionGroup(this)
        .getPartitions()
        .stream()
        .map(partition -> ((LogPartition) partition).getClient().logSessionBuilder().build())
        .collect(Collectors.toList());
    return new DistributedLogClient(this, partitions, config.getPartitioner());
  }

  @Override
  public <S> ProxyClient<S> newProxy(
      String primitiveName,
      PrimitiveType primitiveType,
      Class<S> serviceType,
      ServiceConfig serviceConfig,
      PartitionService partitionService) {
    return new LogProxyClient<S>(primitiveName, primitiveType, this, serviceType, serviceConfig, newClient(partitionService));
  }
}
