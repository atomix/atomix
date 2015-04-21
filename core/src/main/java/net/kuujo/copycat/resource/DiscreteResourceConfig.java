/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.protocol.Protocol;

/**
 * Discrete resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class DiscreteResourceConfig extends ResourceConfig {
  private Protocol protocol;
  private ReplicationStrategy replicationStrategy;
  private CopycatSerializer serializer;

  /**
   * Sets the resource protocol.
   *
   * @param protocol The resource protocol.
   */
  protected void setProtocol(Protocol protocol) {
    this.protocol = protocol;
  }

  /**
   * Returns the resource protocol.
   *
   * @return The resource protocol.
   */
  public Protocol getProtocol() {
    return protocol;
  }

  /**
   * Sets the resource replication strategy.
   *
   * @param replicationStrategy The resource replication strategy.
   */
  protected void setReplicationStrategy(ReplicationStrategy replicationStrategy) {
    this.replicationStrategy = replicationStrategy;
  }

  /**
   * Returns the resource replication strategy.
   *
   * @return The resource replication strategy.
   */
  public ReplicationStrategy getReplicationStrategy() {
    return replicationStrategy;
  }

  /**
   * Sets the resource serializer.
   *
   * @param serializer The resource serializer.
   */
  protected void setSerializer(CopycatSerializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Returns the resource serializer.
   *
   * @return The resource serializer.
   */
  public CopycatSerializer getSerializer() {
    return serializer;
  }

  @Override
  protected DiscreteResourceConfig resolve() {
    if (protocol == null)
      throw new ConfigurationException("protocol not configured");
    if (replicationStrategy == null)
      replicationStrategy = new FullReplicationStrategy();
    if (serializer == null)
      serializer = new CopycatSerializer();
    super.resolve();
    return this;
  }

}
