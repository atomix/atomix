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

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.spi.protocol.Protocol;
import net.kuujo.copycat.spi.protocol.ProtocolClient;

/**
 * Remote node manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RemoteNode<M extends Member> extends ClusterNode<M> {
  private final ProtocolClient client;

  public RemoteNode(Protocol<M> protocol, M member) {
    super(member);
    this.client = protocol.createClient(member);
  }

  /**
   * Returns the protocol client connecting to this node.
   *
   * @return The node's protocol client.
   */
  public ProtocolClient client() {
    return client;
  }

}
