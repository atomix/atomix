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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.util.internal.Assert;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protocol server registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ProtocolServerRegistry {
  private final Protocol protocol;
  private final Map<URI, ProtocolServerCoordinator> servers = new ConcurrentHashMap<>();

  public ProtocolServerRegistry(Protocol protocol) {
    this.protocol = Assert.isNotNull(protocol, "protocol");
  }

  public ProtocolServerCoordinator get(URI uri) {
    return servers.computeIfAbsent(uri, u -> new ProtocolServerCoordinator(protocol.createServer(u)));
  }

}
