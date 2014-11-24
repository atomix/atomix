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
import net.kuujo.copycat.spi.protocol.ProtocolClient;

/**
 * Remote node reference.<p>
 *
 * This type provides the interface for interacting with a remote note by exposing the appripriate
 * {@link net.kuujo.copycat.spi.protocol.ProtocolClient} instance.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RemoteNode extends Node {
  private final ProtocolClient client;

  public RemoteNode(ClusterMember member, Protocol protocol) {
    super(member);
    this.client = protocol.createClient(member.endpoint());
  }

  /**
   * Returns the protocol client connecting to this node.
   *
   * @return The node's protocol client.
   */
  public ProtocolClient client() {
    return client;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof RemoteNode && ((Node) object).member().equals(member());
  }

  @Override
  public int hashCode() {
    int hashCode = 191;
    hashCode = 37 * hashCode + member().hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("RemoteNode[id=%s]", member().id());
  }

}
