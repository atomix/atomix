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
 * limitations under the License
 */
package io.atomix.util;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.session.Session;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A simple {@link CopycatClient} wrapper that exposes a custom {@link Transport}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class AtomixCopycatClient implements CopycatClient {
  private final CopycatClient client;
  private final Transport transport;

  AtomixCopycatClient(CopycatClient client, Transport transport) {
    this.client = Assert.notNull(client, "client");
    this.transport = Assert.notNull(transport, "transport");
  }

  @Override
  public State state() {
    return client.state();
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> consumer) {
    return client.onStateChange(consumer);
  }

  @Override
  public ThreadContext context() {
    return client.context();
  }

  @Override
  public Transport transport() {
    return transport;
  }

  @Override
  public Serializer serializer() {
    return client.serializer();
  }

  @Override
  public Session session() {
    return client.session();
  }

  @Override
  public <T> CompletableFuture<T> submit(Command<T> command) {
    return client.submit(command);
  }

  @Override
  public <T> CompletableFuture<T> submit(Query<T> query) {
    return client.submit(query);
  }

  @Override
  public Listener<Void> onEvent(String event, Runnable callback) {
    return client.onEvent(event, callback);
  }

  @Override
  public <T> Listener<T> onEvent(String event, Consumer<T> callback) {
    return client.onEvent(event, callback);
  }

  @Override
  public CompletableFuture<CopycatClient> open() {
    return client.open();
  }

  @Override
  public CompletableFuture<CopycatClient> recover() {
    return client.recover();
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  public boolean isOpen() {
    return client.isOpen();
  }

  @Override
  public boolean isClosed() {
    return client.isClosed();
  }

  @Override
  public String toString() {
    return client.toString();
  }

}
