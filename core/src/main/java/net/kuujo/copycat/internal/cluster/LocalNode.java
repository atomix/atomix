/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.ClusterMember;
import net.kuujo.copycat.spi.protocol.Protocol;
import net.kuujo.copycat.spi.protocol.ProtocolServer;

/**
 * Local node reference.<p>
 *
 * This type provides the interface for a local node via the appropriate
 * {@link net.kuujo.copycat.spi.protocol.ProtocolServer} instance.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalNode extends Node {
  private final ProtocolServer server;

  public LocalNode(ClusterMember member, Protocol protocol) {
    super(member);
    this.server = protocol.createServer(member.endpoint());
  }

  /**
   * Returns the node's protocol server receiving messages for this member.
   *
   * @return The node's protocol server.
   */
  public ProtocolServer server() {
    return server;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof LocalNode && ((Node) object).member().equals(member());
  }

  @Override
  public int hashCode() {
    int hashCode = 181;
    hashCode = 37 * hashCode + member().hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("LocalNode[id=%s]", member().id());
  }

}
