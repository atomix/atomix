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
package net.kuujo.copycat.protocol.impl;

import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolInstance;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.ProtocolUri;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.uri.UriInjector;

/**
 * Default protocol instance implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultProtocolInstance implements ProtocolInstance {
  private final UriInjector injector;
  private final Protocol protocol;
  private ProtocolClient client;
  private ProtocolServer server;

  public DefaultProtocolInstance(ProtocolUri uri, Registry registry) {
    this.injector = new UriInjector(uri.getRawUri(), registry);
    this.protocol = injector.inject(uri.getProtocolClass());
  }

  @Override
  public String name() {
    return protocol.name();
  }

  @Override
  public ProtocolClient client() {
    if (client == null) {
      client = injector.inject(protocol.createClient());
    }
    return client;
  }

  @Override
  public ProtocolServer server() {
    if (server == null) {
      server = injector.inject(protocol.createServer());
    }
    return server;
  }

}
