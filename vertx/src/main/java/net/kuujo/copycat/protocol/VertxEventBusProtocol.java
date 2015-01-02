/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.protocol;

import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxFactory;

import java.net.URI;

/**
 * Vert.x event bus protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocol extends AbstractProtocol {
  private final Vertx vertx;

  public VertxEventBusProtocol(String host, int port) {
    this(VertxFactory.newVertx(port, host));
  }

  public VertxEventBusProtocol(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public ProtocolClient createClient(URI uri) {
    return new VertxEventBusProtocolClient(uri.getAuthority(), vertx);
  }

  @Override
  public ProtocolServer createServer(URI uri) {
    return new VertxEventBusProtocolServer(uri.getAuthority(), vertx);
  }

}
