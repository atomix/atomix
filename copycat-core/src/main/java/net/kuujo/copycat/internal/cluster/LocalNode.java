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
import net.kuujo.copycat.internal.protocol.AsyncProtocolServerWrapper;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;
import net.kuujo.copycat.spi.protocol.AsyncProtocolServer;
import net.kuujo.copycat.spi.protocol.BaseProtocol;
import net.kuujo.copycat.spi.protocol.Protocol;

/**
 * Local node reference.<p>
 *
 * This type provides the interface for a local node via the appropriate
 * {@link net.kuujo.copycat.spi.protocol.ProtocolServer} instance.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalNode<M extends Member> extends ClusterNode<M> {
  private final AsyncProtocolServer server;

  @SuppressWarnings("unchecked")
  public LocalNode(M member, BaseProtocol<M> protocol) {
    super(member);
    if (protocol instanceof AsyncProtocol) {
      this.server = ((AsyncProtocol<M>) protocol).createServer(member);
    } else {
      this.server = new AsyncProtocolServerWrapper(((Protocol<M>) protocol).createServer(member));
    }
  }

  /**
   * Returns the node's protocol server receiving messages for this member.
   *
   * @return The node's protocol server.
   */
  public AsyncProtocolServer server() {
    return server;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof LocalNode && ((ClusterNode<?>) object).member().equals(member());
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
