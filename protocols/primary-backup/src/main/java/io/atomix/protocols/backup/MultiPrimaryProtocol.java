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
package io.atomix.protocols.backup;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.impl.DefaultProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionClient;
import io.atomix.protocols.backup.partition.PrimaryBackupPartition;
import io.atomix.utils.config.ConfigurationException;

import java.util.Collection;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Multi-primary protocol.
 */
public class MultiPrimaryProtocol implements ProxyProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns an instance of the multi-primary protocol with the default configuration.
   *
   * @return an instance of the multi-primary protocol with the default configuration
   */
  public static MultiPrimaryProtocol instance() {
    return new MultiPrimaryProtocol(new MultiPrimaryProtocolConfig());
  }

  /**
   * Returns a new multi-primary protocol builder.
   *
   * @return a new multi-primary protocol builder
   */
  public static MultiPrimaryProtocolBuilder builder() {
    return new MultiPrimaryProtocolBuilder(new MultiPrimaryProtocolConfig());
  }

  /**
   * Returns a new multi-primary protocol builder for the given group.
   *
   * @param group the partition group
   * @return a new multi-primary protocol builder for the given group
   */
  public static MultiPrimaryProtocolBuilder builder(String group) {
    return new MultiPrimaryProtocolBuilder(new MultiPrimaryProtocolConfig().setGroup(group));
  }

  /**
   * Multi-primary protocol type.
   */
  public static final class Type implements PrimitiveProtocol.Type<MultiPrimaryProtocolConfig> {
    private static final String NAME = "multi-primary";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public MultiPrimaryProtocolConfig newConfig() {
      return new MultiPrimaryProtocolConfig();
    }

    @Override
    public PrimitiveProtocol newProtocol(MultiPrimaryProtocolConfig config) {
      return new MultiPrimaryProtocol(config);
    }
  }

  protected final MultiPrimaryProtocolConfig config;

  protected MultiPrimaryProtocol(MultiPrimaryProtocolConfig config) {
    this.config = config;
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
  public <S> ProxyClient<S> newProxy(String primitiveName, PrimitiveType primitiveType, Class<S> serviceType, ServiceConfig serviceConfig, PartitionService partitionService) {
    PartitionGroup partitionGroup = partitionService.getPartitionGroup(this);
    if (partitionGroup == null) {
      throw new ConfigurationException("No Raft partition group matching the configured protocol exists");
    }

    Collection<SessionClient> partitions = partitionGroup.getPartitions().stream()
        .map(partition -> ((PrimaryBackupPartition) partition).getClient()
            .sessionBuilder(primitiveName, primitiveType, serviceConfig)
            .withConsistency(config.getConsistency())
            .withReplication(config.getReplication())
            .withRecovery(config.getRecovery())
            .withNumBackups(config.getBackups())
            .withMaxRetries(config.getMaxRetries())
            .withRetryDelay(config.getRetryDelay())
            .build())
        .collect(Collectors.toList());
    return new DefaultProxyClient<>(primitiveName, primitiveType, this, serviceType, partitions, config.getPartitioner());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type())
        .add("group", group())
        .toString();
  }
}
