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

import net.kuujo.copycat.protocol.AbstractProtocol;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.util.internal.Assert;

import java.net.URI;
import java.util.concurrent.Executor;

/**
 * Coordinated protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedProtocol extends AbstractProtocol {
  private final int id;
  private final Protocol protocol;
  private final ProtocolServerRegistry registry;
  private final Executor executor;

  public CoordinatedProtocol(int id, Protocol protocol, ProtocolServerRegistry registry, Executor executor) {
    this.id = id;
    this.protocol = Assert.isNotNull(protocol, "protocol");
    this.registry = Assert.isNotNull(registry, "registry");
    this.executor = Assert.isNotNull(executor, "executor");
  }

  @Override
  public ProtocolClient createClient(URI uri) {
    return new CoordinatedProtocolClient(id, protocol.createClient(uri));
  }

  @Override
  public ProtocolServer createServer(URI uri) {
    return new CoordinatedProtocolServer(id, registry.get(uri), executor);
  }

}
