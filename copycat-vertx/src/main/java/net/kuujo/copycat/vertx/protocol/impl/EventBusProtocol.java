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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.uri.Optional;
import net.kuujo.copycat.uri.UriArgument;
import net.kuujo.copycat.uri.UriAuthority;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriInject;
import net.kuujo.copycat.uri.UriPath;
import net.kuujo.copycat.uri.UriPort;
import net.kuujo.copycat.uri.UriSchemeSpecificPart;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultVertx;

/**
 * Vert.x event bus protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocol implements Protocol {
  private Vertx vertx;
  private String address;
  private EventBusProtocolServer server;
  private EventBusProtocolClient client;

  @UriInject
  public EventBusProtocol(@UriArgument("vertx") Vertx vertx, @UriAuthority @UriSchemeSpecificPart String address) {
    this.vertx = vertx;
    this.address = address;
  }

  @UriInject
  public EventBusProtocol(@UriHost String host, @Optional @UriPort int port, @UriPath String address) {
    final CountDownLatch latch = new CountDownLatch(1);
    vertx = new DefaultVertx(port >= 0 ? port : 0, host, new Handler<AsyncResult<Vertx>>() {
      @Override
      public void handle(AsyncResult<Vertx> result) {
        latch.countDown();
      }
    });
    try {
      latch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new ProtocolException(e);
    }
    this.address = address;
  }

  @Override
  public void init(CopyCatContext context) {
    server = new EventBusProtocolServer(address, vertx);
    client = new EventBusProtocolClient(address, vertx);
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
