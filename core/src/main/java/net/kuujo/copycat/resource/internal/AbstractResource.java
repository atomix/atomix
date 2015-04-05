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
package net.kuujo.copycat.resource.internal;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.ResourceState;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractResource<T extends Resource<T>> implements Resource<T> {
  protected final ResourceContext context;
  protected final CopycatSerializer serializer;

  protected AbstractResource(ResourceContext context) {
    if (context == null)
      throw new NullPointerException("context cannot be null");
    this.context = context;
    this.serializer = context.serializer();
  }

  @Override
  public String name() {
    return context.name();
  }

  @Override
  public ResourceState state() {
    return context.state();
  }

  @Override
  public Cluster cluster() {
    return context.cluster();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> open() {
    return context.open().thenApply(v -> (T) this);
  }

  @Override
  public boolean isOpen() {
    return context.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return context.close();
  }

  @Override
  public boolean isClosed() {
    return context.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[context=%s]", getClass().getSimpleName(), context);
  }

}
