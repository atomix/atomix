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
package net.kuujo.copycat.vertx.protocol.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;

import org.vertx.java.core.Vertx;

/**
 * Vert.x event bus protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocol implements Protocol {
  private EventBusProtocolServer server;
  private EventBusProtocolClient client;

  @Override
  public void init(String address, CopyCatContext context) {
    server = new EventBusProtocolServer(address, context.registry().lookup("vertx", Vertx.class));
    client = new EventBusProtocolClient(address, context.registry().lookup("vertx", Vertx.class));
  }

  @Override
  public ProtocolServer server() {
    return server;
  }

  @Override
  public ProtocolClient client() {
    return client;
  }

}
