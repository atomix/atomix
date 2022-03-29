// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.impl.DefaultProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionClient;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.utils.config.ConfigurationException;

import java.util.Collection;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multi-Raft protocol.
 */
public class MultiRaftProtocol implements ProxyProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns an instance of the multi-Raft protocol with the default configuration.
   *
   * @return an instance of the multi-Raft protocol with the default configuration
   */
  public static MultiRaftProtocol instance() {
    return new MultiRaftProtocol(new MultiRaftProtocolConfig());
  }

  /**
   * Returns a new multi-Raft protocol builder.
   *
   * @return a new multi-Raft protocol builder
   */
  public static MultiRaftProtocolBuilder builder() {
    return new MultiRaftProtocolBuilder(new MultiRaftProtocolConfig());
  }

  /**
   * Returns a new multi-Raft protocol builder.
   *
   * @param group the partition group
   * @return the multi-Raft protocol builder
   */
  public static MultiRaftProtocolBuilder builder(String group) {
    return new MultiRaftProtocolBuilder(new MultiRaftProtocolConfig().setGroup(group));
  }

  /**
   * Multi-Raft protocol type.
   */
  public static final class Type implements PrimitiveProtocol.Type<MultiRaftProtocolConfig> {
    private static final String NAME = "multi-raft";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public MultiRaftProtocolConfig newConfig() {
      return new MultiRaftProtocolConfig();
    }

    @Override
    public PrimitiveProtocol newProtocol(MultiRaftProtocolConfig config) {
      return new MultiRaftProtocol(config);
    }
  }

  private final MultiRaftProtocolConfig config;

  protected MultiRaftProtocol(MultiRaftProtocolConfig config) {
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
  public <S> ProxyClient<S> newProxy(String primitiveName, PrimitiveType primitiveType, Class<S> serviceType, ServiceConfig serviceConfig, PartitionService partitionService) {
    PartitionGroup partitionGroup = partitionService.getPartitionGroup(this);
    if (partitionGroup == null) {
      throw new ConfigurationException("No Raft partition group matching the configured protocol exists");
    }

    Collection<SessionClient> partitions = partitionGroup.getPartitions().stream()
        .map(partition -> ((RaftPartition) partition).getClient()
            .sessionBuilder(primitiveName, primitiveType, serviceConfig)
            .withMinTimeout(config.getMinTimeout())
            .withMaxTimeout(config.getMaxTimeout())
            .withReadConsistency(config.getReadConsistency())
            .withCommunicationStrategy(config.getCommunicationStrategy())
            .withRecoveryStrategy(config.getRecoveryStrategy())
            .withMaxRetries(config.getMaxRetries())
            .withRetryDelay(config.getRetryDelay())
            .build())
        .collect(Collectors.toList());
    return new DefaultProxyClient<>(primitiveName, primitiveType, this, serviceType, partitions, config.getPartitioner());
  }
}
