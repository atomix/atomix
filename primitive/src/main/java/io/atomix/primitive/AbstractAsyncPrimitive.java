// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract base class for primitives that interact with Raft replicated state machines via proxy.
 */
public abstract class AbstractAsyncPrimitive<A extends AsyncPrimitive, S> implements AsyncPrimitive {
  private final ProxyClient<S> client;
  private final PrimitiveRegistry registry;

  protected AbstractAsyncPrimitive(ProxyClient<S> client, PrimitiveRegistry registry) {
    this.client = checkNotNull(client, "proxy cannot be null");
    this.registry = checkNotNull(registry, "registry cannot be null");
    client.register(this);
  }

  @Override
  public String name() {
    return client.name();
  }

  @Override
  public PrimitiveType type() {
    return client.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return client.protocol();
  }

  /**
   * Returns the primitive proxy client.
   *
   * @return the primitive proxy client
   */
  protected ProxyClient<S> getProxyClient() {
    return client;
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    client.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    client.removeStateChangeListener(listener);
  }

  /**
   * Connects the primitive.
   *
   * @return a future to be completed once the primitive has been connected
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<A> connect() {
    return registry.createPrimitive(name(), type())
        .thenApply(v -> (A) this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return client.delete().thenCompose(v -> registry.deletePrimitive(name()));
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("proxy", client)
        .toString();
  }
}
