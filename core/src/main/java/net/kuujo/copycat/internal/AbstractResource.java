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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.Resource;
import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.internal.util.Assert;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractResource<T extends Resource> extends AbstractManagedResource<T> implements Resource {
  protected final ResourceContext context;

  protected AbstractResource(ResourceContext context) {
    this.context = Assert.isNotNull(context, "context");
  }

  @Override
  public String name() {
    return context.name();
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    return super.open().thenCompose(v -> context.open());
  }

  @Override
  public boolean isOpen() {
    return context.isOpen();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return context.close().thenCompose(v -> super.close());
  }

  @Override
  public boolean isClosed() {
    return context.isClosed();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return context.delete();
  }

}
